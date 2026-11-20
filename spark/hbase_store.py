"""Batch writes of compact Binance per-symbol state to HBase over Thrift."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping

import happybase


STATE_COLUMNS = (
    ("latest", "exchange_close_time_ms", "exchange_close_time_ms"),
    ("latest", "last_price", "last_price"),
    ("latest", "price_change_percent", "price_change_percent"),
    ("latest", "quote_volume", "quote_volume"),
    ("latest", "trade_count", "trade_count"),
    ("latest", "snapshot_interval_seconds", "snapshot_interval_seconds"),
    ("latest", "price_return_percent", "price_return_percent"),
    ("latest", "net_trade_count_delta", "net_trade_count_delta"),
    ("latest", "net_trade_count_per_second", "net_trade_count_per_second"),
    ("latest", "net_quote_volume_delta", "net_quote_volume_delta"),
    ("latest", "net_quote_volume_per_second", "net_quote_volume_per_second"),
    ("latest", "snapshot_comparison_status", "snapshot_comparison_status"),
    ("latest", "poll_id", "poll_id"),
    ("latest", "poll_received_at", "poll_received_at"),
    ("latest", "response_bytes", "response_bytes"),
    ("latest", "source_message_key", "source_message_key"),
    ("latest", "kafka_timestamp", "kafka_timestamp"),
    ("meta", "kafka_timestamp_ms", "kafka_timestamp_ms"),
    ("latest", "kafka_partition", "kafka_partition"),
    ("latest", "kafka_offset", "kafka_offset"),
)


def encode_symbol_state(
    row: Mapping[str, Any],
) -> tuple[bytes, dict[bytes, bytes], int]:
    """Encode one normalized ticker row and its stable Kafka cell timestamp."""
    symbol = str(row.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("HBase symbol-state row is missing symbol")

    close_time = row.get("exchange_close_time_ms")
    if close_time is None:
        raise ValueError(f"HBase symbol-state row for {symbol} is missing close time")
    close_time = int(close_time)
    if close_time <= 0:
        raise ValueError(f"HBase symbol-state row for {symbol} has invalid close time")

    kafka_timestamp = row.get("kafka_timestamp_ms")
    if kafka_timestamp is None:
        raise ValueError(f"HBase symbol-state row for {symbol} is missing Kafka time")
    kafka_timestamp = int(kafka_timestamp)
    if kafka_timestamp <= 0:
        raise ValueError(f"HBase symbol-state row for {symbol} has invalid Kafka time")

    values = {}
    for family, qualifier, field in STATE_COLUMNS:
        value = row.get(field)
        if isinstance(value, datetime):
            value = value.isoformat()
        encoded = "" if value is None else str(value)
        values[f"{family}:{qualifier}".encode("utf-8")] = encoded.encode("utf-8")

    return symbol.encode("utf-8"), values, kafka_timestamp


class HBaseStateWriter:
    """Bulk-upsert latest accepted state with replay and stale-row guards."""

    def __init__(
        self,
        host: str,
        port: int = 9090,
        table_name: str = "crypto_scam_radar:symbol_state",
        batch_size: int = 10000,
    ) -> None:
        if not host.strip():
            raise ValueError("HBase Thrift host must not be empty")
        if not 1 <= port <= 65535:
            raise ValueError("HBase Thrift port must be between 1 and 65535")
        if not table_name.strip():
            raise ValueError("HBase table name must not be empty")
        if batch_size < 1:
            raise ValueError("HBase batch size must be positive")

        self._table_name = table_name
        self._batch_size = batch_size
        self._connection = happybase.Connection(host=host, port=port, timeout=10000)
        existing_tables = set(self._connection.tables())
        encoded_table_name = table_name.encode("utf-8")
        if encoded_table_name not in existing_tables:
            self._connection.close()
            raise RuntimeError(
                f"HBase table {table_name!r} does not exist; create it before "
                "starting the Spark stream"
            )
        self._table = self._connection.table(encoded_table_name)

    def write(self, rows: Iterable[Mapping[str, Any]]) -> int:
        """Bulk-upsert newer symbol states; retries and stale rows are ignored."""
        updates = {}
        for row in rows:
            record = row.asDict(recursive=False) if hasattr(row, "asDict") else row
            row_key, values, timestamp = encode_symbol_state(record)
            close_time = int(values[b"latest:exchange_close_time_ms"])
            current = updates.get(row_key)
            if current is None or close_time > current[2]:
                updates[row_key] = (values, timestamp, close_time)

        if not updates:
            return 0

        existing_rows = dict(
            self._table.rows(
                list(updates),
                columns=[
                    b"latest:exchange_close_time_ms",
                    b"meta:kafka_timestamp_ms",
                ],
            )
        )
        timestamp_groups = {}
        for row_key, (values, timestamp, close_time) in updates.items():
            existing = existing_rows.get(row_key, {})
            existing_close_time = existing.get(b"latest:exchange_close_time_ms")
            existing_kafka_timestamp = existing.get(b"meta:kafka_timestamp_ms")
            if existing_close_time and int(existing_close_time) >= close_time:
                continue
            if existing_kafka_timestamp and int(existing_kafka_timestamp) >= timestamp:
                continue
            timestamp_groups.setdefault(timestamp, []).append((row_key, values))

        count = 0
        for timestamp, group in timestamp_groups.items():
            # HappyBase assigns one HBase cell timestamp per batch. Kafka
            # timestamps are shared by records from the same NiFi poll.
            with self._table.batch(
                timestamp=timestamp, batch_size=self._batch_size
            ) as batch:
                for row_key, values in group:
                    batch.put(row_key, values)
                    count += 1
        return count

    def close(self) -> None:
        self._connection.close()
