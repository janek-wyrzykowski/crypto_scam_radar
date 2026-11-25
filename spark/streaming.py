#!/usr/bin/env python3
"""Normalize NiFi's Binance and CertStream Kafka records and forward them."""

from __future__ import annotations

import argparse
from typing import Iterator, Sequence

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


MARKET_TOPIC = "crypto-market"
# NiFi publishes CertStream domain names to cyber-certs. Keep this list aligned
# with the actual NiFi flow so the consumer does not subscribe to stale topics.
CERTIFICATE_TOPIC = "cyber-certs"
SUPPORTED_TOPICS = (MARKET_TOPIC, CERTIFICATE_TOPIC)
# Keep the current anomaly-detection run focused on Binance. Certificate
# domains can be processed independently with certificates_streaming.py.
DEFAULT_TOPICS = (MARKET_TOPIC,)
# The two feeds have different fields, so parse each flat JSON object into a
# string-valued map first. Numeric values are cast only for market fields below.
RECORD_SCHEMA = MapType(StringType(), StringType(), valueContainsNull=True)
DELTA_FIELDS = (
    StructField("snapshot_interval_seconds", DoubleType(), True),
    StructField("price_return_percent", DoubleType(), True),
    StructField("net_trade_count_delta", LongType(), True),
    StructField("net_trade_count_per_second", DoubleType(), True),
    StructField("net_quote_volume_delta", DoubleType(), True),
    StructField("net_quote_volume_per_second", DoubleType(), True),
    StructField("snapshot_comparison_status", StringType(), False),
)


def normalize_records(kafka_records: DataFrame) -> DataFrame:
    """Return one validated output schema for Binance and CertStream records.

    NiFi publishes one ticker JSON object or one certificate domain per Kafka
    message. Keep the original value alongside parsed fields for diagnosis.
    """
    # Spark's Kafka source exposes the topic and message value. Convert the
    # binary value to text once, then parse the JSON object into a generic map.
    # Keep Kafka transport metadata for measuring ingestion and diagnosing
    # retries/gaps. The message key can carry a NiFi poll/request ID when set.
    headers = F.map_from_entries(F.col("headers"))
    records = kafka_records.select(
        F.col("topic").alias("source_topic"),
        F.col("key").cast("string").alias("source_message_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.length("value").alias("message_bytes"),
        F.element_at(headers, F.lit("poll_id")).cast("string").alias("poll_id"),
        F.element_at(headers, F.lit("poll_received_at"))
        .cast("string")
        .alias("poll_received_at"),
        F.element_at(headers, F.lit("response_bytes"))
        .cast("string")
        .cast("long")
        .alias("response_bytes"),
        F.col("value").cast("string").alias("raw_value"),
    ).withColumn("record", F.from_json("raw_value", RECORD_SCHEMA))

    # The topic distinguishes Binance's JSON objects from CertStream's plain
    # domain-name values.
    is_market = F.col("source_topic") == MARKET_TOPIC
    symbol = F.upper(F.trim(F.col("record").getItem("symbol")))
    last_price = F.col("record").getItem("lastPrice").cast("double")
    price_change = F.col("record").getItem("priceChangePercent").cast("double")
    quote_volume = F.col("record").getItem("quoteVolume").cast("double")
    trade_count = F.col("record").getItem("count").cast("long")
    close_time = F.col("record").getItem("closeTime").cast("long")
    certificate_domain = F.regexp_replace(
        F.lower(F.trim(F.col("raw_value"))), r"\.$", ""
    )

    # Build an array of only the applicable errors. A malformed record is
    # retained with is_valid=false rather than dropped from the stream.
    validation_errors = F.filter(
        F.array(
            F.when(
                is_market & F.col("record").isNull(),
                F.lit("invalid_json"),
            ),
            F.when(
                is_market & (symbol.isNull() | (symbol == "")),
                F.lit("missing_symbol"),
            ),
            F.when(
                is_market & (last_price.isNull() | (last_price <= 0)),
                F.lit("invalid_last_price"),
            ),
            F.when(
                is_market & price_change.isNull(),
                F.lit("invalid_price_change_percent"),
            ),
            F.when(
                is_market & (quote_volume.isNull() | (quote_volume < 0)),
                F.lit("invalid_quote_volume"),
            ),
            F.when(
                is_market & (trade_count.isNull() | (trade_count < 0)),
                F.lit("invalid_trade_count"),
            ),
            F.when(
                is_market & (close_time.isNull() | (close_time <= 0)),
                F.lit("invalid_close_time"),
            ),
            F.when(
                ~is_market
                & (certificate_domain.isNull() | (certificate_domain == "")),
                F.lit("missing_domain"),
            ),
            F.when(
                ~is_market & (F.length(certificate_domain) > 253),
                F.lit("domain_too_long"),
            ),
            F.when(
                ~is_market & certificate_domain.rlike(r"\s"),
                F.lit("domain_contains_whitespace"),
            ),
        ),
        lambda error: error.isNotNull(),
    )

    # Return stable, typed fields that downstream consumers can query without
    # re-parsing raw_value. Fields belonging to the other feed are null.
    return records.select(
        "source_topic",
        "source_message_key",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        "message_bytes",
        "poll_id",
        "poll_received_at",
        "response_bytes",
        F.when(is_market, F.lit("market_ticker"))
        .otherwise(F.lit("certificate_domain"))
        .alias("event_type"),
        F.current_timestamp().alias("processed_at"),
        (F.size(validation_errors) == 0).alias("is_valid"),
        validation_errors.alias("validation_errors"),
        F.when(is_market, symbol).alias("symbol"),
        F.when(is_market, last_price).alias("last_price"),
        F.when(is_market, price_change).alias("price_change_percent"),
        F.when(is_market, quote_volume).alias("quote_volume"),
        F.when(is_market, trade_count).alias("trade_count"),
        F.when(is_market, close_time).alias("exchange_close_time_ms"),
        F.when(~is_market, certificate_domain).alias("domain"),
        "raw_value",
    )


def add_market_snapshot_deltas(records: DataFrame) -> DataFrame:
    """Compare each valid market snapshot with the last one for its symbol.

    Spark stores the previous snapshot in checkpointed per-symbol state. The
    volume and trade-count differences are net changes to Binance's rolling
    24-hour totals, not exact counts or volume for the interval.
    """
    output_schema = StructType(list(records.schema.fields) + list(DELTA_FIELDS))

    def compare_group(
        _key: tuple, batches: Iterator[pd.DataFrame], state
    ) -> Iterator[pd.DataFrame]:
        previous = None
        if state.exists:
            close_time, last_price, trade_count, quote_volume = state.get
            previous = {
                "close_time": int(close_time),
                "last_price": float(last_price),
                "trade_count": int(trade_count),
                "quote_volume": float(quote_volume),
            }

        # A symbol may have multiple snapshots in one micro-batch. Sort those
        # records by exchange time before updating its state.
        frames = list(batches)
        if not frames:
            return
        rows = pd.concat(frames, ignore_index=True).sort_values(
            "exchange_close_time_ms", kind="stable"
        )
        output_rows = []
        for row in rows.to_dict(orient="records"):
            current = {
                "close_time": int(row["exchange_close_time_ms"]),
                "last_price": float(row["last_price"]),
                "trade_count": int(row["trade_count"]),
                "quote_volume": float(row["quote_volume"]),
            }
            deltas = {
                "snapshot_interval_seconds": None,
                "price_return_percent": None,
                "net_trade_count_delta": None,
                "net_trade_count_per_second": None,
                "net_quote_volume_delta": None,
                "net_quote_volume_per_second": None,
            }

            if previous is None:
                status = "baseline"
                previous = current
            elif current["close_time"] <= previous["close_time"]:
                # Do not let a replayed or late snapshot move state backwards.
                status = "duplicate_or_out_of_order"
            else:
                interval = (current["close_time"] - previous["close_time"]) / 1000.0
                trade_delta = current["trade_count"] - previous["trade_count"]
                volume_delta = current["quote_volume"] - previous["quote_volume"]
                deltas = {
                    "snapshot_interval_seconds": interval,
                    "price_return_percent": (
                        current["last_price"] / previous["last_price"] - 1.0
                    )
                    * 100.0,
                    "net_trade_count_delta": trade_delta,
                    "net_trade_count_per_second": trade_delta / interval,
                    "net_quote_volume_delta": volume_delta,
                    "net_quote_volume_per_second": volume_delta / interval,
                }
                status = "compared"
                previous = current

            output_rows.append(
                {
                    **row,
                    **deltas,
                    "snapshot_comparison_status": status,
                }
            )

        if previous is not None:
            state.update(
                (
                    previous["close_time"],
                    previous["last_price"],
                    previous["trade_count"],
                    previous["quote_volume"],
                )
            )
        yield pd.DataFrame(output_rows, columns=output_schema.fieldNames())

    # Invalid market rows and certificate rows do not participate in the
    # per-symbol state, but retain the same output schema for the Kafka sink.
    market_valid = records.filter(
        (F.col("source_topic") == MARKET_TOPIC) & F.col("is_valid")
    )
    stateful_market = market_valid.groupBy("symbol").applyInPandasWithState(
        compare_group,
        output_schema,
        StructType(
            [
                StructField("close_time", LongType(), False),
                StructField("last_price", DoubleType(), False),
                StructField("trade_count", LongType(), False),
                StructField("quote_volume", DoubleType(), False),
            ]
        ),
        "Append",
        "NoTimeout",
    )

    passthrough = records.filter(
        (F.col("source_topic") != MARKET_TOPIC) | ~F.col("is_valid")
    )
    for field in DELTA_FIELDS[:-1]:
        passthrough = passthrough.withColumn(
            field.name, F.lit(None).cast(field.dataType)
        )
    passthrough = passthrough.withColumn(
        "snapshot_comparison_status",
        F.when(F.col("source_topic") == MARKET_TOPIC, F.lit("invalid")).otherwise(
            F.lit("not_applicable")
        ),
    )
    return stateful_market.unionByName(passthrough)


def select_latest_market_state(batch: DataFrame) -> DataFrame:
    """Select one newest accepted snapshot per symbol for the HBase state table."""
    return (
        batch.filter(
            (F.col("source_topic") == MARKET_TOPIC)
            & F.col("is_valid")
            & F.col("snapshot_comparison_status").isin("baseline", "compared")
        )
        .withColumn(
            "_state_rank",
            F.row_number().over(
                Window.partitionBy("symbol").orderBy(
                    F.col("exchange_close_time_ms").desc(),
                    F.col("kafka_timestamp").desc(),
                    F.col("kafka_partition").desc(),
                    F.col("kafka_offset").desc(),
                )
            ),
        )
        .filter(F.col("_state_rank") == 1)
        .drop("_state_rank")
        .withColumn(
            "kafka_timestamp_ms",
            F.unix_millis("kafka_timestamp"),
        )
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse runtime settings and reject input/output topic feedback loops."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default="127.0.0.1:9092",
        help="Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--topics",
        default=",".join(DEFAULT_TOPICS),
        help="Comma-separated input topics (default: %(default)s)",
    )
    parser.add_argument(
        "--output-topic",
        default="crypto-processed",
        help="Kafka topic for normalized records (default: %(default)s)",
    )
    parser.add_argument(
        "--checkpoint-location",
        default="file:///tmp/crypto-scam-radar-checkpoint-deltas",
        help="Durable Structured Streaming checkpoint path",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default="latest",
        help="Initial Kafka offsets; ignored when resuming an existing checkpoint",
    )
    parser.add_argument(
        "--trigger-interval",
        default="10 seconds",
        help="Micro-batch interval (default: %(default)s)",
    )
    parser.add_argument(
        "--console-rows",
        type=int,
        default=20,
        help="Maximum rows printed per micro-batch (default: %(default)s)",
    )
    parser.add_argument(
        "--hbase-thrift-host",
        default=None,
        help="Enable HBase state writes through this HBase Thrift host",
    )
    parser.add_argument(
        "--hbase-thrift-port",
        type=int,
        default=9090,
        help="HBase Thrift port (default: %(default)s)",
    )
    parser.add_argument(
        "--hbase-state-table",
        default="crypto_scam_radar:symbol_state",
        help="HBase table for per-symbol state (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    if args.console_rows < 0:
        parser.error("--console-rows must be zero or greater")
    # Remove whitespace around user-provided topic names before validating
    # them and passing the list to Kafka's subscribe option.
    args.topics = tuple(topic.strip() for topic in args.topics.split(",") if topic.strip())
    if not args.topics:
        parser.error("--topics must contain at least one topic")
    unsupported_topics = sorted(set(args.topics) - set(SUPPORTED_TOPICS))
    if unsupported_topics:
        parser.error(
            "--topics contains unsupported topic(s): " + ", ".join(unsupported_topics)
        )
    if args.output_topic in args.topics:
        parser.error("--output-topic must not be one of the input topics")
    if not 1 <= args.hbase_thrift_port <= 65535:
        parser.error("--hbase-thrift-port must be between 1 and 65535")
    if not args.hbase_state_table.strip():
        parser.error("--hbase-state-table must not be empty")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("crypto-scam-radar").getOrCreate()
    hbase_state_writer = None
    if args.hbase_thrift_host:
        if __package__:
            from .hbase_store import HBaseStateWriter
        else:
            from hbase_store import HBaseStateWriter

        hbase_state_writer = HBaseStateWriter(
            host=args.hbase_thrift_host,
            port=args.hbase_thrift_port,
            table_name=args.hbase_state_table,
        )
    topic_list = ",".join(args.topics)
    # Structured Streaming resumes offsets from its checkpoint after the first
    # run. The explicit startingOffsets value is used only for a new checkpoint.
    kafka_records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("kafka.group.id", "crypto-market-spark")
        .option("subscribe", topic_list)
        .option("startingOffsets", args.starting_offsets)
        .option("includeHeaders", "true")
        # Do not let a typo or stale NiFi topic name create a new broker topic.
        .option("kafka.allow.auto.create.topics", "false")
        .load()
    )
    normalized = add_market_snapshot_deltas(normalize_records(kafka_records))

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        # Both show() and the Kafka write are actions. Persist this micro-batch
        # so Spark evaluates the source and normalization only once.
        batch.persist()
        try:
            # Report what Spark actually received in this micro-batch. These
            # bytes count Kafka values only, not Kafka protocol overhead or
            # the full pre-SplitJson HTTP response.
            totals = batch.agg(
                F.count("*").alias("records"),
                F.sum("message_bytes").alias("payload_bytes"),
            ).first()
            # Console output is intended for development/inspection; zero rows
            # disables it without disabling publication to the output topic.
            print(
                f"Micro-batch {batch_id}: records={totals.records}, "
                f"Kafka value bytes={totals.payload_bytes or 0}"
            )
            # A request ID used as the Kafka key lets us measure how many
            # split ticker records and bytes came from each NiFi poll.
            poll_totals = (
                batch.filter(F.col("source_message_key").isNotNull())
                .groupBy("source_message_key")
                .agg(
                    F.count("*").alias("records"),
                    F.sum("message_bytes").alias("payload_bytes"),
                )
                .collect()
            )
            for poll in poll_totals:
                print(
                    f"  poll_id={poll.source_message_key}: "
                    f"records={poll.records}, Kafka value bytes={poll.payload_bytes}"
                )
            if hbase_state_writer:
                latest_state = select_latest_market_state(batch).toLocalIterator()
                written = hbase_state_writer.write(latest_state)
                print(f"  HBase symbol-state upserts={written}")
            if args.console_rows:
                batch.show(args.console_rows, truncate=False)
            # Kafka expects a string value. Serialize the complete normalized
            # row, including validation errors and the original JSON payload.
            (
                batch.select(
                    F.to_json(F.struct(*[F.col(name) for name in batch.columns])).alias(
                        "value"
                    )
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap_servers)
                .option("topic", args.output_topic)
                .save()
            )
        finally:
            # Release cached data even if display or Kafka publication fails.
            batch.unpersist()

    # foreachBatch lets the same micro-batch be displayed and written to Kafka
    # under one checkpointed streaming query.
    query = (
        normalized.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        if hbase_state_writer:
            hbase_state_writer.close()


if __name__ == "__main__":
    main()
