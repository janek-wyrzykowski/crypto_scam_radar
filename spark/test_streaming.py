import unittest

from pyspark.sql import SparkSession

from streaming import normalize_records, parse_args


class NormalizeRecordsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # A local single-worker Spark session exercises real Spark SQL
        # expressions without requiring the Kafka connector or a broker.
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("crypto-scam-radar-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def normalize(self, topic, value):
        # Match the columns emitted by Spark's Kafka source that the
        # normalization function consumes.
        source = self.spark.createDataFrame([(topic, value)], ["topic", "value"])
        return normalize_records(source).first()

    def test_normalizes_market_ticker(self):
        # Binance sends numeric values as JSON numbers or strings; the output
        # should normalize numeric values and canonicalize the symbol.
        result = self.normalize(
            "crypto-market",
            '{"symbol":" btcusdt ","lastPrice":"65000.25",'
            '"priceChangePercent":"2.5","quoteVolume":"1000","count":"42"}',
        )

        self.assertTrue(result.is_valid)
        self.assertEqual(result.event_type, "market_ticker")
        self.assertEqual(result.symbol, "BTCUSDT")
        self.assertEqual(result.last_price, 65000.25)
        self.assertEqual(result.price_change_percent, 2.5)
        self.assertEqual(result.trade_count, 42)

    def test_normalizes_certificate_from_nifi_topic(self):
        # The installed NiFi flow publishes crt.sh records to cyber-certs.
        result = self.normalize(
            "cyber-certs",
            '{"id":"123","common_name":"example.crypto",'
            '"name_value":"example.crypto","not_before":"2026-01-01"}',
        )

        self.assertTrue(result.is_valid)
        self.assertEqual(result.event_type, "certificate")
        self.assertEqual(result.certificate_id, "123")
        self.assertEqual(result.common_name, "example.crypto")

    def test_marks_invalid_records_without_dropping_them(self):
        # Preserve the input even when JSON parsing fails so the downstream
        # output topic can act as a simple dead-letter/diagnostic stream.
        result = self.normalize("cyber-certs", "{not-json")

        self.assertFalse(result.is_valid)
        self.assertIn("invalid_json", result.validation_errors)
        self.assertEqual(result.raw_value, "{not-json")

    def test_reports_missing_and_unparseable_market_fields(self):
        # A record is invalid if required ticker identifiers are absent or
        # required numeric fields cannot be converted to numbers.
        result = self.normalize(
            "crypto-market",
            '{"symbol":"","lastPrice":"unknown","priceChangePercent":"bad"}',
        )

        self.assertFalse(result.is_valid)
        self.assertCountEqual(
            result.validation_errors,
            ("missing_symbol", "invalid_last_price", "invalid_price_change_percent"),
        )

    def test_rejects_unsupported_topics_and_input_output_loops(self):
        # Only the two NiFi source topics are valid, and publishing back into
        # an input topic would cause the stream to consume its own output.
        args = parse_args(["--topics", "crypto-market,cyber-certs"])
        self.assertEqual(args.topics, ("crypto-market", "cyber-certs"))

        with self.assertRaises(SystemExit):
            parse_args(["--topics", "crypto-cert"])
        with self.assertRaises(SystemExit):
            parse_args(["--topics", "crypto-market", "--output-topic", "crypto-market"])


if __name__ == "__main__":
    unittest.main()
