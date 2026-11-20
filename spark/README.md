# Spark streaming module

This PySpark Structured Streaming job currently focuses on Binance ticker
snapshots from the `crypto-market` Kafka topic. It normalizes records, marks
invalid messages, computes per-symbol snapshot metrics, prints each
micro-batch, and publishes output to Kafka. Original JSON is retained in
`raw_value` for downstream analysis. The certificate topic remains an optional
input for later work, but is not consumed by default.

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

The output preserves Kafka's message key, broker timestamp, partition, offset,
and value size, along with NiFi's poll ID, poll-received timestamp, and original
HTTP response size from Kafka headers. Spark reports record count and total
value bytes per micro-batch and per poll. `kafka_timestamp` is the record's
Kafka timestamp, not the time the HTTP request started.

The inspected NiFi flow polls Binance every 10 seconds and splits its JSON
array before publishing to `crypto-market`. It also has a certificate path to
`cyber-certs`, which this Binance-focused job does not subscribe to by default.
Kafka's automatic input-topic creation is disabled so a stale or misspelled
topic is not created.

## NiFi ingestion metadata and failure visibility

The verified Binance path is `InvokeHTTP` (10-second schedule) ->
`UpdateAttribute` -> `SplitJson` -> `PublishKafka`. The attributes are set on
the HTTP response before splitting: `poll_id=${uuid}`,
`poll_received_at=${now():format(...)}`, and `response_bytes=${fileSize}`.
`PublishKafka` uses `${poll_id}` as its Kafka key and forwards these attributes
as headers matching `^(poll_id|poll_received_at|response_bytes)$`. Thus, all
ticker records from one response have the same key and repeat the same poll
headers.

`response_bytes` describes the pre-split HTTP response and is repeated on each
resulting ticker record; do not sum it across records in a poll.
`message_bytes`, in contrast, is the size of one split Kafka value. The
`poll_received_at` header is recorded after the response arrives, so it is not
the HTTP request start time.

The Binance publisher uses `acks=all`. The current `crypto-market` topic has
replication factor 1 on a single broker, so this acknowledges the only replica
and does not provide broker redundancy. InvokeHTTP `Failure`, `Retry`, and
`No Retry` relationships route to logging; `Retry` is logged rather than
re-attempted, so a failed poll waits for the next scheduled request. SplitJson
and Kafka publish failures are also logged.

Spark enables Kafka header inclusion and parses the three headers into the
normalized output. Messages produced before the NiFi metadata was configured
may not have these headers; Spark leaves the corresponding output fields null.

## HBase state tables

HBase is the intended database for compact serving state; the full raw ticker
stream is not archived under the current database-only policy. The
`crypto_scam_radar` namespace now contains:

| Table | Row granularity | Column families |
| --- | --- | --- |
| `symbol_state` | One row per Binance symbol | `latest`, `baseline`, `meta` |
| `anomaly_events` | One row per emitted candidate event | `signal`, `source` |

Each family currently retains one version. `symbol_state` will hold the latest
accepted snapshot and that symbol's baseline state. `anomaly_events` will hold
only suspicious candidates and their supporting source/features. The tables
have been created and verified. When enabled, Spark bulk-upserts only the
latest accepted row per symbol into `symbol_state`; it does not persist every
input record. HBase cell timestamps use Kafka record timestamps for replay-safe
writes; the writer also compares exchange close times before upserting, so an
older snapshot cannot replace a newer one. The baseline and anomaly-event
writers are separate future behavior.

Start HBase Thrift bound to localhost in a separate terminal:

```bash
HBASE_CONF_DIR=/opt/bigdata/hbase/conf \
  /opt/bigdata/hbase/bin/hbase thrift start -b 127.0.0.1 -p 9090
```

Enable the state writer by passing `--hbase-thrift-host 127.0.0.1
--hbase-thrift-port 9090` to `streaming.py`. The `happybase` client is installed
with `spark/requirements.txt`; without the host option, the existing Kafka-only
behavior is unchanged.

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
  --topics crypto-market \
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
  "source_message_key": "poll-id-from-nifi-or-null",
  "kafka_timestamp": "2026-11-20T11:40:00.123Z",
  "kafka_partition": 0,
  "kafka_offset": 41,
  "message_bytes": 246,
  "poll_id": "poll-id-from-nifi-or-null",
  "poll_received_at": "2026-11-20T11:40:00.100+01:00",
  "response_bytes": 1913232,
  "event_type": "market_ticker",
  "processed_at": "2026-11-20T11:40:01.111Z",
  "is_valid": true,
  "validation_errors": [],
  "symbol": "MATICBTC",
  "last_price": 0.00000667,
  "price_change_percent": 0.0,
  "quote_volume": 0.00297615,
  "trade_count": 1,
  "exchange_close_time_ms": 1795174800000,
  "snapshot_interval_seconds": null,
  "price_return_percent": null,
  "net_trade_count_delta": null,
  "net_trade_count_per_second": null,
  "net_quote_volume_delta": null,
  "net_quote_volume_per_second": null,
  "snapshot_comparison_status": "baseline",
  "raw_value": "{\"symbol\":\"MATICBTC\",\"lastPrice\":\"0.00000667\", ...}"
}
```

`source_message_key` and `poll_id` should match for records from the current
NiFi flow. `response_bytes` is copied from a header and repeated for every
ticker in a poll; `message_bytes` measures only this split ticker value.

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
