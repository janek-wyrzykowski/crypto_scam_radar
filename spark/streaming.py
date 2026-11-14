#!/usr/bin/env python3
"""Normalize NiFi's Binance and crt.sh Kafka records and forward them to Kafka."""

from __future__ import annotations

import argparse
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType


MARKET_TOPIC = "crypto-market"
# NiFi publishes the crt.sh feed to cyber-certs. Keep this list aligned with
# the actual NiFi flow so the consumer does not subscribe to stale topic names.
CERTIFICATE_TOPIC = "cyber-certs"
DEFAULT_TOPICS = (MARKET_TOPIC, CERTIFICATE_TOPIC)
# The two feeds have different fields, so parse each flat JSON object into a
# string-valued map first. Numeric values are cast only for market fields below.
RECORD_SCHEMA = MapType(StringType(), StringType(), valueContainsNull=True)


def normalize_records(kafka_records: DataFrame) -> DataFrame:
    """Return one validated output schema for Binance and crt.sh Kafka records.

    NiFi's SplitJson processors publish one JSON object per Kafka message. Keep
    the original message alongside parsed fields so malformed or not-yet-known
    properties are still available for diagnosis and downstream processing.
    """
    # Spark's Kafka source exposes the topic and message value. Convert the
    # binary value to text once, then parse the JSON object into a generic map.
    records = kafka_records.select(
        F.col("topic").alias("source_topic"),
        F.col("value").cast("string").alias("raw_value"),
    ).withColumn("record", F.from_json("raw_value", RECORD_SCHEMA))

    # The topic is the discriminator between the market and certificate
    # schemas; all configured inputs besides crypto-market are cyber-certs.
    is_market = F.col("source_topic") == MARKET_TOPIC
    symbol = F.upper(F.trim(F.col("record").getItem("symbol")))
    last_price = F.col("record").getItem("lastPrice").cast("double")
    price_change = F.col("record").getItem("priceChangePercent").cast("double")
    certificate_id = F.col("record").getItem("id")
    common_name = F.trim(F.col("record").getItem("common_name"))
    name_value = F.trim(F.col("record").getItem("name_value"))

    # Build an array of only the applicable errors. A malformed record is
    # retained with is_valid=false rather than dropped from the stream.
    validation_errors = F.filter(
        F.array(
            F.when(F.col("record").isNull(), F.lit("invalid_json")),
            F.when(
                is_market & (symbol.isNull() | (symbol == "")),
                F.lit("missing_symbol"),
            ),
            F.when(
                is_market & last_price.isNull(),
                F.lit("invalid_last_price"),
            ),
            F.when(
                is_market & price_change.isNull(),
                F.lit("invalid_price_change_percent"),
            ),
            F.when(
                ~is_market
                & (
                    certificate_id.isNull()
                    | (F.trim(certificate_id) == "")
                ),
                F.lit("missing_certificate_id"),
            ),
            F.when(
                ~is_market & (common_name.isNull() | (common_name == "")),
                F.lit("missing_common_name"),
            ),
            F.when(
                ~is_market & (name_value.isNull() | (name_value == "")),
                F.lit("missing_name_value"),
            ),
        ),
        lambda error: error.isNotNull(),
    )

    # Return stable, typed fields that downstream consumers can query without
    # re-parsing raw_value. Fields belonging to the other feed are null.
    return records.select(
        "source_topic",
        F.when(is_market, F.lit("market_ticker"))
        .otherwise(F.lit("certificate"))
        .alias("event_type"),
        F.current_timestamp().alias("processed_at"),
        (F.size(validation_errors) == 0).alias("is_valid"),
        validation_errors.alias("validation_errors"),
        F.when(is_market, symbol).alias("symbol"),
        F.when(is_market, last_price).alias("last_price"),
        F.when(is_market, price_change).alias("price_change_percent"),
        F.when(is_market, F.col("record").getItem("quoteVolume").cast("double"))
        .alias("quote_volume"),
        F.when(is_market, F.col("record").getItem("count").cast("long"))
        .alias("trade_count"),
        F.when(~is_market, certificate_id).alias("certificate_id"),
        F.when(~is_market, common_name).alias("common_name"),
        F.when(~is_market, name_value).alias("name_value"),
        F.when(~is_market, F.col("record").getItem("not_before"))
        .alias("not_before"),
        F.when(~is_market, F.col("record").getItem("not_after"))
        .alias("not_after"),
        "raw_value",
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
        default="file:///tmp/crypto-scam-radar-checkpoint",
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
    args = parser.parse_args(argv)
    if args.console_rows < 0:
        parser.error("--console-rows must be zero or greater")
    # Remove whitespace around user-provided topic names before validating
    # them and passing the list to Kafka's subscribe option.
    args.topics = tuple(topic.strip() for topic in args.topics.split(",") if topic.strip())
    if not args.topics:
        parser.error("--topics must contain at least one topic")
    unsupported_topics = sorted(set(args.topics) - set(DEFAULT_TOPICS))
    if unsupported_topics:
        parser.error(
            "--topics contains unsupported topic(s): " + ", ".join(unsupported_topics)
        )
    if args.output_topic in args.topics:
        parser.error("--output-topic must not be one of the input topics")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("crypto-scam-radar").getOrCreate()
    topic_list = ",".join(args.topics)
    # Structured Streaming resumes offsets from its checkpoint after the first
    # run. The explicit startingOffsets value is used only for a new checkpoint.
    kafka_records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", topic_list)
        .option("startingOffsets", args.starting_offsets)
        # Do not let a typo or stale NiFi topic name create a new broker topic.
        .option("kafka.allow.auto.create.topics", "false")
        .load()
    )
    normalized = normalize_records(kafka_records)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        # Both show() and the Kafka write are actions. Persist this micro-batch
        # so Spark evaluates the source and normalization only once.
        batch.persist()
        try:
            # Console output is intended for development/inspection; zero rows
            # disables it without disabling publication to the output topic.
            print(f"Micro-batch {batch_id}")
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
    (
        normalized.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=args.trigger_interval)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
