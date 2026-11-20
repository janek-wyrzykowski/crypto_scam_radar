import unittest

from spark.hbase_store import encode_symbol_state


class EncodeSymbolStateTest(unittest.TestCase):
    def test_encodes_symbol_state_with_kafka_time_version(self):
        row = {
            "symbol": " btcusdt ",
            "exchange_close_time_ms": 1700000000000,
            "last_price": 65000.25,
            "price_change_percent": 2.5,
            "quote_volume": 1000.0,
            "trade_count": 42,
            "snapshot_comparison_status": "compared",
            "poll_id": "poll-123",
            "poll_received_at": "2026-11-20T13:13:18.393+01:00",
            "response_bytes": 1913232,
            "source_message_key": "poll-123",
            "kafka_timestamp_ms": 1795190000000,
            "kafka_partition": 1,
            "kafka_offset": 41,
        }

        row_key, values, timestamp = encode_symbol_state(row)

        self.assertEqual(row_key, b"BTCUSDT")
        self.assertEqual(timestamp, 1795190000000)
        self.assertEqual(values[b"latest:last_price"], b"65000.25")
        self.assertEqual(values[b"latest:response_bytes"], b"1913232")
        self.assertEqual(values[b"latest:snapshot_interval_seconds"], b"")
        self.assertEqual(values[b"latest:poll_id"], b"poll-123")

    def test_replayed_row_encodes_identically(self):
        row = {
            "symbol": "BTCUSDT",
            "exchange_close_time_ms": 1700000000000,
            "kafka_timestamp_ms": 1795190000000,
            "last_price": 65000.25,
        }

        self.assertEqual(encode_symbol_state(row), encode_symbol_state(row))

    def test_rejects_missing_symbol_or_close_time(self):
        with self.assertRaisesRegex(ValueError, "missing symbol"):
            encode_symbol_state({"exchange_close_time_ms": 1700000000000})
        with self.assertRaisesRegex(ValueError, "missing close time"):
            encode_symbol_state({"symbol": "BTCUSDT"})
        with self.assertRaisesRegex(ValueError, "missing Kafka time"):
            encode_symbol_state(
                {
                    "symbol": "BTCUSDT",
                    "exchange_close_time_ms": 1700000000000,
                }
            )
