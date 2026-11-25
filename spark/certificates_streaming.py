#!/usr/bin/env python3
"""Normalize CertStream domain events from Kafka and publish them to Kafka."""

from __future__ import annotations

import argparse
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

if __package__:
    from .streaming import CERTIFICATE_TOPIC, normalize_records
else:
    from streaming import CERTIFICATE_TOPIC, normalize_records


CERTIFICATE_OUTPUT_FIELDS = (
    "source_topic",
    "source_message_key",
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
    "message_bytes",
    "poll_id",
    "poll_received_at",
    "response_bytes",
    "event_type",
    "processed_at",
    "is_valid",
    "validation_errors",
    "domain",
    "raw_value",
)


def normalize_certificate_records(kafka_records: DataFrame) -> DataFrame:
    """Keep the certificate feed's normalized fields and discard market rows."""
    return (
        normalize_records(kafka_records)
        .filter(F.col("source_topic") == CERTIFICATE_TOPIC)
        .select(*CERTIFICATE_OUTPUT_FIELDS)
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse options for the standalone certificate-domain stream."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default="127.0.0.1:9092",
        help="Kafka bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--output-topic",
        default="cyber-certs-processed",
        help="Kafka topic for normalized certificate domains (default: %(default)s)",
    )
    parser.add_argument(
        "--checkpoint-location",
        default="file:///tmp/crypto-scam-radar-certificates-checkpoint",
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
    if args.output_topic == CERTIFICATE_TOPIC:
        parser.error("--output-topic must not be the input topic")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("crypto-scam-radar-certificates").getOrCreate()
    kafka_records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("kafka.group.id", "cyber-certs-spark")
        .option("subscribe", CERTIFICATE_TOPIC)
        .option("startingOffsets", args.starting_offsets)
        .option("includeHeaders", "true")
        .option("kafka.allow.auto.create.topics", "false")
        .load()
    )
    normalized = normalize_certificate_records(kafka_records)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        """Log each micro-batch and publish normalized domain messages."""
        batch.persist()
        try:
            count = batch.count()
            print(f"Micro-batch {batch_id}: certificate-domain records={count}")
            if args.console_rows:
                batch.show(args.console_rows, truncate=False)
            (
                batch.select(
                    F.to_json(
                        F.struct(*[F.col(name) for name in batch.columns])
                    ).alias("value")
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap_servers)
                .option("topic", args.output_topic)
                .save()
            )
        finally:
            batch.unpersist()

    query = (
        normalized.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
