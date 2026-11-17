import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from streaming import add_market_snapshot_deltas, normalize_records, parse_args


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
            '{"symbol":" btcusdt ","lastPrice":65000.25,'
            '"priceChangePercent":2.5,"quoteVolume":1000,"count":42,'
            '"closeTime":1700000000000}',
        )

        self.assertTrue(result.is_valid)
        self.assertEqual(result.event_type, "market_ticker")
        self.assertEqual(result.symbol, "BTCUSDT")
        self.assertEqual(result.last_price, 65000.25)
        self.assertEqual(result.price_change_percent, 2.5)
        self.assertEqual(result.trade_count, 42)
        self.assertEqual(result.exchange_close_time_ms, 1700000000000)

    def test_normalizes_certificate_from_nifi_topic(self):
        # The installed NiFi flow publishes crt.sh records to cyber-certs.
        result = self.normalize(
            "cyber-certs",
            '{"id":123,"common_name":"example.crypto",'
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
            '{"symbol":"","lastPrice":"unknown","priceChangePercent":"bad",'
            '"quoteVolume":"not-a-number","count":"bad","closeTime":"bad"}',
        )

        self.assertFalse(result.is_valid)
        self.assertCountEqual(
            result.validation_errors,
            (
                "missing_symbol",
                "invalid_last_price",
                "invalid_price_change_percent",
                "invalid_quote_volume",
                "invalid_trade_count",
                "invalid_close_time",
            ),
        )

    def test_compares_market_snapshots_and_tracks_net_rolling_deltas(self):
        # A file stream gives the stateful transformation two separate
        # micro-batches, exercising checkpointed per-symbol state transitions.
        with tempfile.TemporaryDirectory() as input_dir:
            source = (
                self.spark.readStream.schema("topic STRING, value STRING")
                .json(input_dir)
            )
            output = add_market_snapshot_deltas(normalize_records(source))
            query_name = "market_snapshot_delta_test"
            query = (
                output.writeStream.format("memory")
                .queryName(query_name)
                .outputMode("append")
                .start()
            )

            def write_snapshot(filename, close_time, last_price, count, volume):
                payload = {
                    "symbol": "BTCUSDT",
                    "lastPrice": str(last_price),
                    "priceChangePercent": "2.0",
                    "quoteVolume": str(volume),
                    "count": str(count),
                    "closeTime": str(close_time),
                }
                message = {
                    "topic": "crypto-market",
                    "value": json.dumps(payload),
                }
                Path(input_dir, filename).write_text(json.dumps(message) + "\n")
                query.processAllAvailable()

            try:
                write_snapshot("snapshot-1.json", 1700000000000, 100, 100, 10000)
                certificate = {
                    "id": 123,
                    "common_name": "example.crypto",
                    "name_value": "example.crypto",
                }
                Path(input_dir, "certificate.json").write_text(
                    json.dumps(
                        {
                            "topic": "cyber-certs",
                            "value": json.dumps(certificate),
                        }
                    )
                    + "\n"
                )
                query.processAllAvailable()
                write_snapshot("snapshot-2.json", 1700000010000, 102, 110, 10100)
                write_snapshot("snapshot-3.json", 1700000020000, 101, 108, 9900)
                write_snapshot("snapshot-4.json", 1700000015000, 103, 120, 11000)
                rows = self.spark.sql(
                    f"SELECT * FROM {query_name} "
                    "WHERE event_type = 'market_ticker' "
                    "ORDER BY exchange_close_time_ms"
                ).collect()
                certificate_rows = self.spark.sql(
                    f"SELECT * FROM {query_name} WHERE event_type = 'certificate'"
                ).collect()
            finally:
                query.stop()

        self.assertEqual(
            [row.snapshot_comparison_status for row in rows],
            ["baseline", "compared", "duplicate_or_out_of_order", "compared"],
        )
        self.assertIsNone(rows[0].price_return_percent)
        self.assertAlmostEqual(rows[1].snapshot_interval_seconds, 10.0)
        self.assertAlmostEqual(rows[1].price_return_percent, 2.0)
        self.assertEqual(rows[1].net_trade_count_delta, 10)
        self.assertAlmostEqual(rows[1].net_trade_count_per_second, 1.0)
        self.assertAlmostEqual(rows[1].net_quote_volume_delta, 100.0)
        self.assertAlmostEqual(rows[1].net_quote_volume_per_second, 10.0)
        # A rolling 24-hour total can shrink as old activity leaves the window.
        self.assertEqual(rows[3].net_trade_count_delta, -2)
        self.assertEqual(rows[3].net_quote_volume_delta, -200.0)
        self.assertEqual(len(certificate_rows), 1)
        self.assertEqual(
            certificate_rows[0].snapshot_comparison_status, "not_applicable"
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
