# Spark streaming module

This module reads cryptocurrency ticker messages from Kafka, checks and
normalizes them with Spark, and publishes the results back to Kafka. It can
also keep the latest accepted ticker for each symbol in HBase.

The pipeline is a data-processing foundation, not a scam classifier: it
calculates changes between ticker snapshots but does not yet label coins as
safe or suspicious.

## The pipeline in plain language

| Component | What it does |
| --- | --- |
| **Binance** | Provides ticker snapshots. Each snapshot contains rolling 24-hour totals. |
| **CertStream** | Streams newly observed domain names from Certificate Transparency activity. The current feed provides domain names, not full certificate records. |
| **NiFi** | Polls Binance every 10 seconds and splits each response into one message per symbol. Its certificate flow receives CertStream events and publishes domain names. |
| **Kafka** | Holds messages so Spark can read them: `crypto-market` for tickers and `cyber-certs` for certificate domains. |
| **Spark** | Parses each message, checks required fields, compares snapshots for each symbol, and writes normalized results to Kafka. |
| **HBase (optional)** | Stores the latest accepted state per symbol instead of keeping every incoming message. |

Market-data path:

```text
Binance -> NiFi -> Kafka (crypto-market) -> Spark
                                           |-> Kafka (crypto-processed)
                                           `-> HBase (optional)
```

Certificate-domain path:

```text
CertStream server -> python/forwarder.py -> NiFi -> Kafka (cyber-certs)
                                                   -> Spark -> Kafka (cyber-certs-processed)
```

The market and certificate feeds have separate Spark entry points below.

A **poll** is one scheduled request NiFi makes to Binance. It produces many
messages—one per symbol—and those messages share a `poll_id`. This lets you
group ticker records that came from the same Binance response.

## What Spark calculates

For each market message, Spark preserves the original JSON and Kafka/NiFi
metadata, parses fields such as symbol, price, quote volume, and trade count,
and marks invalid messages with `is_valid` and `validation_errors`. Invalid
messages are still included in the output for troubleshooting.

Spark keeps the last accepted snapshot for each symbol in its checkpoint.
The first snapshot is called the **baseline**. Later snapshots are compared
with it to calculate price return and changes in trade count and quote volume.
Because Binance reports rolling 24-hour totals, those count and volume changes
are **net changes in the rolling totals**—not exact activity during the
10-second polling interval. They can be negative when older activity leaves
the 24-hour window.

The Kafka output topic is `crypto-processed`. Each output message is a JSON
object with the parsed fields, comparison results, source metadata, and the
original message in `raw_value`. For example:

```json
{
  "source_topic": "crypto-market",
  "symbol": "BTCUSDT",
  "last_price": 65000.25,
  "snapshot_comparison_status": "compared",
  "net_trade_count_delta": 12,
  "is_valid": true,
  "raw_value": "{\"symbol\":\"BTCUSDT\", ...}"
}
```

The certificate topic contains one plain domain name per message, for example
`www.example.com`. The certificate Spark job trims and lowercases it, removes a
final DNS dot, and performs basic checks (non-empty, at most 253 characters,
and no whitespace). It retains the original domain and Kafka/NiFi metadata.
This job does not receive certificate bodies, issuer details, or validity
dates, so it cannot produce those fields.

## What is stored

The current policy is to avoid archiving every raw ticker message:

- Spark's **checkpoint** remembers stream progress and prior per-symbol
  snapshots. Keep it between runs; a new or deleted checkpoint loses that
  Spark state.
- HBase's `crypto_scam_radar:symbol_state` table has one row per symbol. When
  HBase writing is enabled, Spark upserts the latest accepted snapshot for
  each symbol in a batch. Kafka timestamps and exchange close times prevent
  duplicate or older snapshots from replacing newer state.
- The `baseline` column family and `anomaly_events` table are reserved for
  future baseline statistics and suspicious-event records. Spark does not
  populate them yet.

## Run locally

The server setup used by this project is Spark 3.5.9, Scala 2.12, Kafka 3.6.1,
and Hadoop 3.3.6. The Spark installation does not bundle the Kafka connector,
so `spark-submit` downloads the matching connector package. NiFi and Kafka
must be running before starting the stream.

### 1. Prepare Python

From the repository root, create an environment for the Python packages used
by the Spark job and its Python workers:

```bash
python3 -m venv --system-site-packages "$HOME/.venvs/crypto-scam-radar"
"$HOME/.venvs/crypto-scam-radar/bin/pip" install -r spark/requirements.txt
export PYSPARK_PYTHON="$HOME/.venvs/crypto-scam-radar/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
```

### 2. Create the output topic

Spark writes normalized messages to `crypto-processed`. Create that topic
before starting the job:

```bash
/opt/bigdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic crypto-processed \
  --partitions 1 --replication-factor 1
```

### 3. Start Spark

Run this from the repository root:

```bash
/opt/bigdata/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  spark/streaming.py \
  --bootstrap-servers 127.0.0.1:9092 \
  --topics crypto-market \
  --output-topic crypto-processed \
  --checkpoint-location file:///tmp/crypto-scam-radar-checkpoint-deltas \
  --starting-offsets latest
```

The job keeps running and prints a summary for each micro-batch. Press
`Ctrl+C` to stop it. With `latest`, it starts with messages arriving after the
job starts. To replay retained Kafka messages, use `earliest` and a **new
checkpoint path**. When reusing a checkpoint, Spark resumes its saved offsets
and ignores the starting-offset setting.

### Optional: write latest state to HBase

The HBase tables must exist, and the HBase Thrift service must be running.
On the current server, start the service in another terminal:

```bash
HBASE_CONF_DIR=/opt/bigdata/hbase/conf \
  /opt/bigdata/hbase/bin/hbase thrift start -b 127.0.0.1 -p 9090
```

Add these options to the Spark command to enable HBase writes:

```text
--hbase-thrift-host 127.0.0.1 --hbase-thrift-port 9090
```

Without these options, the stream writes only to Kafka.

## Process certificate domains

The certificate feed has its own Spark script and output topic. First create
the output topic:

```bash
/opt/bigdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic cyber-certs-processed \
  --partitions 2 --replication-factor 1
```

Then start the job from the repository root:

```bash
/opt/bigdata/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  spark/certificates_streaming.py \
  --bootstrap-servers 127.0.0.1:9092 \
  --output-topic cyber-certs-processed \
  --checkpoint-location file:///tmp/crypto-scam-radar-certificates-checkpoint \
  --starting-offsets latest
```

As with the market job, keep the checkpoint between restarts. To replay
retained certificate messages, use `earliest` with a **new checkpoint path**.
The output contains the source message key, Kafka timestamp/partition/offset,
NiFi poll metadata, normalized `domain`, validation status, and original
`raw_value`. Spark sends invalid values to the output with validation errors;
it does not discard them.

## Inspect the output

In another terminal, read a few processed messages:

```bash
/opt/bigdata/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto-processed \
  --from-beginning \
  --max-messages 3
```

For certificate-domain output, replace `crypto-processed` with
`cyber-certs-processed`.

If HBase writing is enabled, inspect a few symbol rows with the HBase shell:

```bash
/opt/bigdata/hbase/bin/hbase shell
```

Then run:

```text
scan 'crypto_scam_radar:symbol_state', {LIMIT => 5}
```

## Run the tests

The unit tests use local Spark and do not need a live Kafka broker or HBase
Thrift service. Run them from the repository root:

```bash
export SPARK_HOME=/opt/bigdata/spark
export PYSPARK_PYTHON="$HOME/.venvs/crypto-scam-radar/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYTHONPATH="$SPARK_HOME/python/lib/pyspark.zip:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PWD"

"$PYSPARK_PYTHON" -m unittest discover \
  -s spark/tests -t . -p 'test_*.py' -v
```

Tests live under `spark/tests/`. They cover record normalization, snapshot
comparisons, certificate-domain normalization, Kafka header handling,
state-row selection, and HBase row encoding.

## Important limitations

- NiFi's Kafka publisher uses `acks=all`, but `crypto-market` currently has
  replication factor 1 on a single broker. This does not provide broker
  redundancy.
- In the inspected NiFi flow, failed HTTP requests are logged; the `Retry`
  relationship is not an actual retry.
- Kafka output is at-least-once. A retried Spark micro-batch can produce
  duplicate output messages, so downstream consumers should tolerate them.
- For a cluster deployment, use a durable checkpoint location shared by the
  Spark workers and a Kafka address reachable from those workers.
