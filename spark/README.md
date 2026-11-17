# Spark streaming module

This PySpark Structured Streaming job consumes the JSON objects published by
NiFi to Kafka, normalizes Binance ticker and crt.sh certificate records, marks
invalid records, prints each micro-batch, and publishes all records to a Kafka
output topic. Original JSON is retained in `raw_value` for downstream analysis.

For market records, it uppercases `symbol` and parses the last price, price
change, quote volume, and trade count. For certificate records, it exposes the
certificate ID, common name, names, and validity dates. `is_valid` and
`validation_errors` describe whether required fields could be parsed; invalid
messages are still forwarded with their original `raw_value`. This is a
normalization and market-snapshot metrics pipeline, not yet a scam-detection
classifier.

For each market symbol, Spark keeps the previous valid snapshot in its
checkpointed state. It calculates `price_return_percent` from the two
`lastPrice` values and reports `net_trade_count_delta` and
`net_quote_volume_delta`, including per-second rates and the actual interval
between exchange timestamps. The first snapshot for each symbol is marked
`baseline`; repeated or out-of-order snapshots are marked and do not move the
saved state. Count and volume deltas are **net changes in Binance's rolling
24-hour totals**, not exact trades or volume during that interval; they may be
negative as older activity leaves the window. Certificate records currently
pass through without being correlated to market anomalies or generating alerts.

The inspected NiFi flow polls both APIs every 10 seconds and splits each JSON
array before publishing to `crypto-market` and `cyber-certs`. The broker
may contain older topic names, but this job subscribes only to the two topics
configured in NiFi: `crypto-market` and `cyber-certs`. Kafka's automatic
input-topic creation is disabled so a stale or misspelled topic is not created.

## Run

The installed server stack is Spark 3.5.9, Scala 2.12, Kafka 3.6.1, and Hadoop
3.3.6. The Spark installation does not include the Kafka connector, so submit
with the matching connector package (Spark resolves it from Maven). Ensure
NiFi and Kafka are running and create the output topic first:

```bash
/opt/bigdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic crypto-processed \
  --partitions 1 --replication-factor 1
```

The stateful Pandas API also requires pandas and PyArrow. Install them in a
Python environment that can access the Spark installation:

```bash
python3 -m venv --system-site-packages "$HOME/.venvs/crypto-scam-radar"
"$HOME/.venvs/crypto-scam-radar/bin/pip" install -r spark/requirements.txt
export PYSPARK_PYTHON="$HOME/.venvs/crypto-scam-radar/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
```

From the repository root, start the stream:

```bash
/opt/bigdata/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  spark/streaming.py \
  --bootstrap-servers 127.0.0.1:9092 \
  --topics crypto-market,cyber-certs \
  --output-topic crypto-processed \
  --checkpoint-location file:///tmp/crypto-scam-radar-checkpoint-deltas \
  --starting-offsets latest
```

The Spark terminal prints each processed micro-batch. To also read the output
topic, run this in another terminal:

```bash
/opt/bigdata/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto-processed \
  --from-beginning
```

With `latest`, the job processes records arriving after startup. To replay
retained input records, use `--starting-offsets earliest` with a **new**
checkpoint path. Reusing the same checkpoint resumes the saved offsets, so
changing `--starting-offsets` alone will not replay data. This version adds
per-symbol state, so use the new `crypto-scam-radar-checkpoint-deltas` path
instead of a checkpoint created by the earlier stateless version.

For a YARN deployment, use a shared durable checkpoint path (for example an
HDFS path) and a cluster-accessible Kafka address. Use a new checkpoint path
when changing input topics or starting-offset behavior; an existing checkpoint
takes precedence over `--starting-offsets`. Also make the pandas/PyArrow
environment available to every executor and set `PYSPARK_PYTHON` to that
environment. Kafka output is at-least-once, so downstream consumers should
tolerate duplicates when a micro-batch is retried.

## Output Format

The job normalizes incoming streams into a unified data envelope. Validated records are published to `crypto-processed` in the following JSON format:

```json
{
  "source_topic": "crypto-market",
  "event_type": "market_ticker",
  "processed_at": "2026-11-14T14:53:57.111+01:00",
  "is_valid": true,
  "validation_errors": [],
  "symbol": "MATICBTC",
  "last_price": 0.00000667,
  "price_change_percent": 0.0,
  "quote_volume": 0.00297615,
  "trade_count": 1,
  "raw_value": "{\"symbol\":\"MATICBTC\",\"priceChange\":\"0.00000000\", ...}"
}

## Tests

Run the unit tests with the server's Spark Python libraries on `PYTHONPATH`:

```bash
cd spark
PYTHONPATH=/opt/bigdata/spark/python:/opt/bigdata/spark/python/lib/py4j-0.10.9.7-src.zip \
PYSPARK_PYTHON="$HOME/.venvs/crypto-scam-radar/bin/python" \
PYSPARK_DRIVER_PYTHON="$HOME/.venvs/crypto-scam-radar/bin/python" \
  "$HOME/.venvs/crypto-scam-radar/bin/python" -m unittest -v
```

The tests cover both input shapes, normalization, invalid-record reporting,
per-symbol snapshot state across micro-batches, and late-snapshot handling.
They do not require a live Kafka broker.
