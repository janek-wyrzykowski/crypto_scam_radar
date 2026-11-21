from datetime import datetime
import json
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.streaming import (
    add_market_snapshot_deltas,
    normalize_records,
    parse_args,
    select_latest_market_state,
)


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

    def normalize(self, topic, value, headers=None):
        # Match the columns emitted by Spark's Kafka source that the
        # normalization function consumes.
        schema = StructType(
            [
                StructField("topic", StringType(), False),
                StructField("key", BinaryType(), True),
                StructField("value", BinaryType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("partition", IntegerType(), False),
                StructField("offset", LongType(), False),
                StructField(
                    "headers",
                    ArrayType(
                        StructType(
                            [
                                StructField("key", StringType(), False),
                                StructField("value", BinaryType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
            ]
        )
        source = self.spark.createDataFrame(
            [
                (
                    topic,
                    b"poll-123",
                    value.encode("utf-8"),
                    datetime(2026, 11, 20, 12, 0, 0),
                    2,
                    41,
                    headers,
                )
            ],
            schema,
        )
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
        self.assertEqual(result.source_message_key, "poll-123")
        self.assertEqual(result.kafka_partition, 2)
        self.assertEqual(result.kafka_offset, 41)
        self.assertEqual(
            result.kafka_timestamp, datetime(2026, 11, 20, 12, 0, 0)
        )
        self.assertEqual(result.message_bytes, len(result.raw_value.encode("utf-8")))
        self.assertIsNone(result.poll_id)
        self.assertIsNone(result.poll_received_at)
        self.assertIsNone(result.response_bytes)

    def test_normalizes_nifi_poll_headers(self):
        result = self.normalize(
            "crypto-market",
            '{"symbol":"BTCUSDT","lastPrice":"65000",'
            '"priceChangePercent":"2.5","quoteVolume":"1000",'
            '"count":"42","closeTime":"1700000000000"}',
            [
                {"key": "poll_id", "value": b"poll-123"},
                {
                    "key": "poll_received_at",
                    "value": b"2026-11-20T13:13:18.393+01:00",
                },
                {"key": "response_bytes", "value": b"1913232"},
            ],
        )

        self.assertEqual(result.source_message_key, "poll-123")
        self.assertEqual(result.poll_id, "poll-123")
        self.assertEqual(
            result.poll_received_at, "2026-11-20T13:13:18.393+01:00"
        )
        self.assertEqual(result.response_bytes, 1913232)

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
                self.spark.readStream.schema(
                    "topic STRING, key STRING, value STRING, timestamp TIMESTAMP, "
                    "partition INT, offset BIGINT, "
                    "headers ARRAY<STRUCT<key: STRING, value: BINARY>>"
                )
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
        default_args = parse_args([])
        self.assertEqual(default_args.topics, ("crypto-market",))

        args = parse_args(["--topics", "crypto-market,cyber-certs"])
        self.assertEqual(args.topics, ("crypto-market", "cyber-certs"))

        with self.assertRaises(SystemExit):
            parse_args(["--topics", "crypto-cert"])
        with self.assertRaises(SystemExit):
            parse_args(["--topics", "crypto-market", "--output-topic", "crypto-market"])

    def test_accepts_hbase_state_writer_options(self):
        args = parse_args(
            [
                "--hbase-thrift-host",
                "127.0.0.1",
                "--hbase-thrift-port",
                "9090",
                "--hbase-state-table",
                "crypto_scam_radar:symbol_state",
            ]
        )

        self.assertEqual(args.hbase_thrift_host, "127.0.0.1")
        self.assertEqual(args.hbase_thrift_port, 9090)
        self.assertEqual(args.hbase_state_table, "crypto_scam_radar:symbol_state")

    def test_selects_latest_accepted_symbol_state(self):
        schema = StructType(
            [
                StructField("source_topic", StringType(), False),
                StructField("is_valid", BooleanType(), False),
                StructField("snapshot_comparison_status", StringType(), False),
                StructField("symbol", StringType(), True),
                StructField("exchange_close_time_ms", LongType(), True),
                StructField("kafka_timestamp", TimestampType(), True),
                StructField("kafka_partition", IntegerType(), True),
                StructField("kafka_offset", LongType(), True),
            ]
        )
        rows = [
            (
                "crypto-market",
                True,
                "baseline",
                "BTCUSDT",
                1700000000000,
                datetime(2026, 11, 20, 12, 0, 0),
                0,
                1,
            ),
            (
                "crypto-market",
                True,
                "compared",
                "BTCUSDT",
                1700000010000,
                datetime(2026, 11, 20, 12, 0, 10),
                0,
                2,
            ),
            (
                "crypto-market",
                True,
                "duplicate_or_out_of_order",
                "BTCUSDT",
                1699999990000,
                datetime(2026, 11, 20, 12, 0, 20),
                0,
                3,
            ),
            (
                "cyber-certs",
                True,
                "not_applicable",
                None,
                None,
                datetime(2026, 11, 20, 12, 0, 30),
                0,
                4,
            ),
        ]
        source = self.spark.createDataFrame(rows, schema)

        selected = select_latest_market_state(source).select(
            "symbol", "exchange_close_time_ms", "kafka_timestamp_ms"
        ).collect()

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].symbol, "BTCUSDT")
        self.assertEqual(selected[0].exchange_close_time_ms, 1700000010000)
        self.assertIsNotNone(selected[0].kafka_timestamp_ms)

    def test_rejects_invalid_hbase_port(self):
        with self.assertRaises(SystemExit):
            parse_args(["--hbase-thrift-port", "70000"])


if __name__ == "__main__":
    unittest.main()
