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
normalization/validation pipeline, not a scam-detection classifier.

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

From the repository root, start the stream:

```bash
/opt/bigdata/spark/bin/spark-submit \
  --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  spark/streaming.py \
  --bootstrap-servers 127.0.0.1:9092 \
  --topics crypto-market,cyber-certs \
  --output-topic crypto-processed \
  --checkpoint-location file:///tmp/crypto-scam-radar-checkpoint \
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
changing `--starting-offsets` alone will not replay data.

For a YARN deployment, use a shared durable checkpoint path (for example an
HDFS path) and a cluster-accessible Kafka address. Use a new checkpoint path
when changing input topics or starting-offset behavior; an existing checkpoint
takes precedence over `--starting-offsets`. Kafka output is at-least-once, so
downstream consumers should tolerate duplicates when a micro-batch is retried.

## Tests

Run the unit tests with the server's Spark Python libraries on `PYTHONPATH`:

```bash
cd spark
PYTHONPATH=/opt/bigdata/spark/python:/opt/bigdata/spark/python/lib/py4j-0.10.9.7-src.zip \
  python3 -m unittest -v
```

The tests cover both input shapes, normalization, and invalid-record reporting;
they do not require a live Kafka broker.
