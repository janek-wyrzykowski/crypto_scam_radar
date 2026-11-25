# Crypto Scam Radar

A Big Data Analytics project at Warsaw University of Technology. The project
collects cryptocurrency market snapshots and Certificate Transparency domain
events, prepares them for analysis, and aims to identify signals associated
with cryptocurrency scams.

**Current scope:** the repository contains data acquisition and streaming
preprocessing. It does not yet produce a trained classifier or definitive
scam verdicts.

## Data sources and flow

```text
Binance ticker API -> NiFi -> Kafka: crypto-market -> Spark -> crypto-processed
                                                          `-> HBase symbol state

CertStream -> Python forwarder -> NiFi -> Kafka: cyber-certs
                                           -> Spark -> cyber-certs-processed
```

| Source | Input topic | Data currently received |
| --- | --- | --- |
| Binance 24-hour ticker API | `crypto-market` | One JSON ticker per symbol. NiFi polls every 10 seconds; volume and trade counts are rolling 24-hour totals. |
| CertStream | `cyber-certs` | One plain domain name per message, with Kafka/NiFi metadata. This is not a full certificate record. |

Spark validates and normalizes market messages, compares snapshots per symbol,
and publishes them to `crypto-processed`. Certificate messages are normalized
by a separate Spark job and published to `cyber-certs-processed`.

The design is **Kappa-style**: Kafka is the stream log and Spark can replay
retained messages using `earliest` with a new checkpoint. The project still
needs to demonstrate this replay and ensure Kafka retention covers the chosen
replay period.

## Storage and analysis status

- Spark checkpoints retain stream progress and market snapshot comparison
  state.
- Optional HBase writes keep the latest accepted snapshot per market symbol;
  the pipeline does not archive every raw ticker.
- HBase baseline statistics and suspicious-event writes are planned but not
  implemented yet.
- Certificate processing currently normalizes domains; it does not enrich
  them, score risk, or store them in HBase.

## Getting started

The Python CertStream setup, configuration, and forwarder instructions are in
[`python/README.md`](python/README.md).

Spark setup, run commands for both streams, Kafka output inspection, HBase
options, and test instructions are in [`spark/README.md`](spark/README.md).

The Spark unit tests run locally without a Kafka broker or HBase service.

## Project requirements

The Milestone 2 requirements—including the expected report, architecture,
dataset descriptions, test documentation, and presentation—are listed in
[`project_metadata/project_milestone_2_requirements.md`](project_metadata/project_milestone_2_requirements.md).
