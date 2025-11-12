# 1) High-level workflow (end-to-end)

Quick summary of the pipeline before we dive into code:

1. **Producer container** runs `producer.py`. It fetches stock quotes (via `yfinance` or simulated), constructs JSON events with `symbol, price, volume, ts, ts_epoch_ms`, and publishes them to a Kafka topic (`events`) using `kafka-python`.
2. **Kafka** (zookeeper+kafka containers) receives and stores messages in the `events` topic.
3. **Spark streaming container** runs `spark/app.py` as a Spark Structured Streaming job:

   * consumes messages from Kafka,
   * parses JSON, creates `event_time`,
   * deduplicates and aggregates into 1-minute tumbling windows (avg/min/max/first/last/pct_change),
   * writes aggregated results to Parquet and CSV sinks (partitioned by date),
   * also writes raw test Parquet/CSV so you can verify write path quickly,
   * console sinks are included for debugging.
4. Parquet/CSV are saved to a host-mounted directory `./data/output` so you can inspect results on the host.

---

# 2) `docker-compose.yml` — line-by-line

I'll show the logical sections and explain each key line. (I omit identical comments like `# ...` where obvious.)

```yaml
# docker-compose.yml  (Compose V2 — no top-level "version")
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.9.3.arm64
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    healthcheck:
      test: ["CMD-SHELL","/usr/bin/zookeeper-shell localhost:2181 ls / >/dev/null 2>&1 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 20
```

* `services:` top-level block defining containers.
* `zookeeper:` service running Confluent Zookeeper image (ARM build). Zookeeper is required by Kafka for metadata.
* `image:` Docker image to pull.
* `container_name:` static name for easy `docker logs` and `exec`.
* `environment:` env vars inside container:

  * `ZOOKEEPER_CLIENT_PORT`: port Zookeeper listens on inside container.
  * `ZOOKEEPER_TICK_TIME`: ZK tick time; default tick tune.
* `ports:` host:container mapping — exposes port `2181` on host.
* `healthcheck:` ensures other services can wait until ZK is ready:

  * `test` executes shell command to test ZK.
  * `interval`, `timeout`, `retries` configure check frequency and tolerance.

```yaml
  kafka:
    image: confluentinc/cp-kafka:7.9.3.arm64
    container_name: kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "9094:9094"
```

* `kafka:` Kafka broker service (Confluent image).
* `depends_on:` ensures `kafka` waits for `zookeeper` healthy status before starting.
* `ports:` exposes broker ports:

  * `9092` internal broker listener
  * `9094` advertised host listener (compose maps both, enabling host clients)

```yaml
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_BROKER_ID: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LISTENERS: PLAINTEXT://:9092,PLAINTEXT_HOST://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LOG_DIRS: /var/lib/kafka/data
```

* Key Kafka env:

  * `KAFKA_ZOOKEEPER_CONNECT` points to the zookeeper service by DNS `zookeeper:2181`.
  * `KAFKA_BROKER_ID` numeric ID (1).
  * `OFFSETS_TOPIC_REPLICATION_FACTOR` etc. set replication factors (single-node dev settings).
  * `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` reduces consumer group delay on start.
  * `KAFKA_LISTENERS` defines broker listeners; one internal (9092) and one for host (9094).
  * `ADVERTISED_LISTENERS` tells clients (internal/host) which hostname/port to use. `PLAINTEXT://kafka:9092` is important for in-network container access (`kafka` resolves to container). `PLAINTEXT_HOST://localhost:9094` allows host tools to reach it via localhost:9094.
  * `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`: mapping of listener names to protocol.
  * `KAFKA_LOG_DIRS`: where Kafka stores topic data inside container.

```yaml
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL","/usr/bin/kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 10s
      timeout: 5s
      retries: 30
```

* `volumes:` maps named volume `kafka-data` to container's Kafka data dir for persistence across container restarts.
* `healthcheck:` verifies Kafka can list topics; used by depends_on in other services.

```yaml
  kafka-init:
    image: confluentinc/cp-kafka:7.9.3.arm64
    depends_on:
      kafka:
        condition: service_healthy
    entrypoint: [ "/bin/bash","-lc" ]
    command: >
      set -euo pipefail;
      echo "Waiting for Kafka at kafka:9092 to accept admin ops...";
      for i in {1..30}; do
        if /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
          echo "Kafka is responding (attempt $$i).";
          break;
        fi;
        echo "Still waiting (attempt $$i).";
        sleep 2;
      done;
      echo "Creating topic '${KAFKA_TOPIC:-events}' if it doesn't exist...";
      /usr/bin/kafka-topics --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic "${KAFKA_TOPIC:-events}" \
        --partitions 3 \
        --replication-factor 1;
      echo "Current topics:";
      /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list
```

* `kafka-init` is a one-shot container to create topics after broker ready.
* Uses `entrypoint` and `command` to run a shell script that:

  * waits for Kafka response,
  * creates topic `${KAFKA_TOPIC:-events}` with partitions=3 (if not exists),
  * lists topics for logs.
* `depends_on` ensures kafka is healthy before running.

```yaml
  spark-master:
    image: spark:3.5.1-python3
    container_name: spark-master
    command: ["/opt/spark/bin/spark-class","org.apache.spark.deploy.master.Master","--host","spark-master"]
    ports:
      - "7077:7077"
      - "8080:8080"
    volumes:
      - ./data/output:/opt/spark-output
```

* `spark-master`: Spark standalone cluster master.
* `command` starts Spark master, sets `--host spark-master` so other containers can resolve it by name.
* Ports: `7077` Spark master RPC, `8080` Spark master UI.
* Mounts `./data/output` to `/opt/spark-output` so master container can view output (optional).

```yaml
  spark-worker:
    image: spark:3.5.1-python3
    container_name: spark-worker
    depends_on: [spark-master]
    command: ["/opt/spark/bin/spark-class","org.apache.spark.deploy.worker.Worker","spark://spark-master:7077"]
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
    ports:
      - "8081:8081"
    volumes:
      - ./data/output:/opt/spark-output
```

* `spark-worker` joins master `spark://spark-master:7077`.
* `SPARK_WORKER_CORES` & `MEMORY` limit resource consumption.
* `8081` worker UI.
* Mounts same output directory so executors can write Parquet files.

```yaml
  spark-streaming:
    image: spark:3.5.1-python3
    container_name: spark-streaming
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
      spark-master:
        condition: service_started
    volumes:
      - ./spark:/opt/spark-app
      - ./data/output:/opt/spark-output
      - ./cache/ivy:/tmp/.ivy2
```

* `spark-streaming`: container that runs `spark-submit` to execute your streaming job (`app.py`).
* `depends_on`: waits for Kafka, kafka-init, and spark-master. Note these condition keys help ordering but are not a substitute for full readiness checks.
* Mounts:

  * `./spark` (your `app.py`) as `/opt/spark-app`.
  * `./data/output` for Parquet/CSV output.
  * `./cache/ivy` to cache Ivy jars.

```yaml
    entrypoint:
      - /bin/bash
      - -lc
      - >
        RUN_ID=$(date +%s);
        export RUN_ID;
        rm -rf /opt/spark-output/parquet/_chk || true;
        mkdir -p /tmp/.ivy2 && mkdir -p /opt/spark-output && chmod -R 777 /opt/spark-output;
        exec /opt/spark/bin/spark-submit
        --master spark://spark-master:7077
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
        --conf spark.sql.shuffle.partitions=4
        --conf spark.jars.ivy=/tmp/.ivy2
        /opt/spark-app/app.py
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
      - OUTPUT_PATH=/opt/spark-output/parquet
      - HOME=/tmp
```

* Custom `entrypoint` runs a shell script to:

  * set `RUN_ID` (not required, but handy in logs),
  * remove an old checkpoint dir (dev convenience),
  * create directories and set permissive permissions,
  * run `spark-submit` with:

    * `--master spark://spark-master:7077` to connect to Spark cluster,
    * `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` to add Kafka connector,
    * `--conf spark.sql.shuffle.partitions=4` reduce default shuffle partitions (tuning for small cluster),
    * `--conf spark.jars.ivy=/tmp/.ivy2` specify Ivy cache path,
    * `/opt/spark-app/app.py` path to job.
* `environment:` passes Kafka settings and `OUTPUT_PATH` into container.

```yaml
  producer:
    build:
      context: ./producer
    container_name: producer
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
      - MSGS_PER_SEC=${MSGS_PER_SEC:-5}
    command: ["python","producer.py"]
```

* `producer` service builds from local `./producer` directory (Dockerfile).
* `depends_on` ensures Kafka ready & topic created before starting.
* Environment variables configure producer behavior.
* `command` runs `producer.py`.

```yaml
volumes:
  kafka-data:
    driver: local
```

* Top-level `volumes:` block defines named volume `kafka-data` used by `kafka` service.

---

# 3) `producer/Dockerfile` — line-by-line

```dockerfile
FROM python:3.11-slim
```

* Base image: Python 3.11 slim variant (lightweight Debian-based image). Good for reproducibility.

```dockerfile
ARG DEBIAN_FRONTEND=noninteractive
ENV DEBIAN_FRONTEND=${DEBIAN_FRONTEND}
WORKDIR /app
```

* `ARG/ENV` disables interactive apt prompts.
* `WORKDIR /app` sets working directory for subsequent commands + final container run.

```dockerfile
RUN set -eux; \
    apt-get update -y || (sleep 2 && apt-get update -y); \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        wget \
    ; \
    rm -rf /var/lib/apt/lists/*
```

* Updates apt and installs minimal packages:

  * `build-essential` in case a wheel must be built (compilation tools).
  * `ca-certificates/wget` for TLS and downloads.
* `--no-install-recommends` keeps image small.
* Removes apt lists to reduce image size.
* Uses retry pattern `|| (sleep 2 && apt-get update -y)` to handle transient network flakiness.

```dockerfile
RUN pip install --upgrade pip setuptools wheel
```

* Upgrades pip & wheel so pip selects appropriate binary wheels (important on aarch64).

```dockerfile
COPY requirements.txt .
```

* Copies requirements into image.

```dockerfile
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt
```

* Installs Python packages. `--prefer-binary` prefers wheels vs building from source; `--no-cache-dir` keeps image smaller.

```dockerfile
COPY producer.py .
```

* Copies producer script into image.

```dockerfile
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=events
ENV TICKERS=AAPL,MSFT,GOOG,AMZN,TSLA
ENV MSGS_PER_SEC=5
```

* Sets sane defaults for runtime env vars; these can be overridden by compose.

```dockerfile
CMD ["python", "producer.py"]
```

* Default command executed when container starts.

---

# 4) `producer/requirements.txt` — explanation

```
kafka-python==2.0.2
yfinance==0.2.31
pandas==2.2.3
numpy==1.25.2
```

* `kafka-python`: lightweight Kafka client used by the producer.
* `yfinance`: convenience wrapper around Yahoo Finance to fetch stock quotes. May be rate-limited; used here for prototyping.
* `pandas` & `numpy`: used by `yfinance` internals and optional data handling; pinned to versions compatible with manylinux aarch64 wheels for your environment.

---

# 5) `producer/producer.py` — line-by-line

I’ll show the code in sections and explain each line. This file creates quotes (live or simulated) and sends to Kafka synchronously.

```python
#!/usr/bin/env python3
"""
Robust Kafka producer for stock tick events.
- Uses yfinance to fetch quotes (falls back to simulation).
- Sends synchronously (fut.get(timeout)) to surface delivery errors.
- Graceful SIGINT/SIGTERM shutdown.
- Configurable via environment variables.
"""
```

* Shebang for direct execution.
* Docstring explains purpose and key behaviors.

```python
import os
import time
import json
import signal
import logging
from datetime import datetime, timezone
import random
```

* Standard lib imports:

  * `os` for env vars,
  * `time` for sleep,
  * `json` to serialize payloads,
  * `signal` for graceful shutdown,
  * `logging` for logs,
  * `datetime` to timestamp events,
  * `random` for simulated quotes.

```python
from kafka import KafkaProducer
from kafka.errors import KafkaError
```

* Import `kafka-python` classes.

```python
try:
    import yfinance as yf  # type: ignore
except Exception:
    yf = None  # will fallback to simulate if needed
```

* Attempt to import `yfinance`. If missing or import error, set `yf=None` so code can fallback to simulated quotes. This makes the producer robust if `yfinance` fails or is not desired.

```python
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")
```

* Configure root logging format and level.
* Get logger instance.

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
MSGS_PER_SEC = float(os.getenv("MSGS_PER_SEC", "5"))
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "AAPL,MSFT,GOOG,AMZN,TSLA").split(",")]
SIMULATE_IF_NO_YFINANCE = os.getenv("SIMULATE_IF_NO_YFINANCE", "true").lower() in ("1", "true", "yes")
SEND_TIMEOUT = float(os.getenv("SEND_TIMEOUT_SEC", "10"))
```

* Reads configuration from environment:

  * `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap string used by kafka-python (container DNS name).
  * `KAFKA_TOPIC`: Kafka topic name.
  * `MSGS_PER_SEC`: producer send rate.
  * `TICKERS`: list of tickers to produce.
  * `SIMULATE_IF_NO_YFINANCE`: whether to fall back to simulation if `yfinance` not available.
  * `SEND_TIMEOUT`: timeout for synchronous send.

```python
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    linger_ms=10,
)
```

* Construct `KafkaProducer`:

  * `bootstrap_servers`: list of brokers.
  * `value_serializer`: serialize payload to JSON bytes.
  * `retries=5`: retry on transient errors.
  * `linger_ms=10`: small buffer time to allow batching.

```python
_shutting_down = False
```

* Global flag used to signal graceful shutdown.

```python
def signal_handler(signum, frame):
    global _shutting_down
    log.info("Received signal %s, shutting down...", signum)
    _shutting_down = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
```

* Register signal handlers for SIGINT and SIGTERM to set `_shutting_down` and allow clean shutdown.

```python
def fetch_quote_live(ticker: str):
    """Try to get a quote using yfinance. Return dict or None on failure."""
    if yf is None:
        return None
    try:
        tk = yf.Ticker(ticker)
        info = getattr(tk, "fast_info", None) or {}
        price = info.get("lastPrice") or info.get("last_price")
        if price is None:
            hist = tk.history(period="1d", interval="1m")
            if not hist.empty:
                price = float(hist["Close"].iloc[-1])
        volume = info.get("lastVolume") or info.get("volume")
        if price is None:
            return None
        return {"price": float(price), "volume": int(volume) if volume is not None else None}
    except Exception as e:
        log.debug("yfinance error for %s: %s", ticker, e)
        return None
```

* Tries to get a quote via `yfinance.Ticker`:

  * Prefers `fast_info` (faster attributes) and tries `lastPrice` or `last_price`.
  * Falls back to using `history` to get the last close price.
  * Returns `None` if no price; logs debug info on exceptions (not noisy).

```python
def fetch_quote_simulated(ticker: str):
    base = random.uniform(100, 1000)
    return {"price": round(base + random.uniform(-1, 1), 2), "volume": int(random.uniform(100, 10000))}
```

* Generates a simulated quote with random price and volume for offline testing or fallback.

```python
def fetch_quote(ticker: str):
    # prefer live when available
    if yf is not None:
        v = fetch_quote_live(ticker)
        if v:
            return v
        if not SIMULATE_IF_NO_YFINANCE:
            return None
    # fallback simulate
    return fetch_quote_simulated(ticker)
```

* Wrapper that uses live if available; otherwise simulate.

```python
def build_payload(ticker: str, quote: dict):
    ts = datetime.now(timezone.utc)
    return {
        "symbol": ticker,
        "price": float(quote["price"]),
        "volume": int(quote["volume"]) if quote.get("volume") is not None else None,
        "ts": ts.isoformat(),
        "ts_epoch_ms": int(ts.timestamp() * 1000),
    }
```

* Builds the JSON payload with ISO timestamp (`ts`) and epoch ms (`ts_epoch_ms`). `ts_epoch_ms` is handy to deduplicate and for numeric comparisons.

```python
def send_sync(topic: str, payload: dict, timeout: float = SEND_TIMEOUT):
    """Send synchronously and return metadata or raise."""
    try:
        fut = producer.send(topic, value=payload)
        meta = fut.get(timeout=timeout)
        return meta
    except KafkaError as e:
        raise
```

* Sends message synchronously (`fut.get()` blocks until broker ack or timeout). Good for dev to surface delivery errors; in production you may want async sends for throughput.

```python
def main():
    interval = 1.0 / MSGS_PER_SEC if MSGS_PER_SEC > 0 else 1.0
    log.info("Producer starting. Brokers=%s topic=%s tickers=%s rate=%.2f/s",
             KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, TICKERS, MSGS_PER_SEC)
    count = 0
    try:
        while not _shutting_down:
            for ticker in TICKERS:
                if _shutting_down:
                    break
                quote = fetch_quote(ticker)
                if quote is None:
                    log.warning("No quote for %s (skipping)", ticker)
                    time.sleep(interval)
                    continue
                payload = build_payload(ticker, quote)
```

* Main loop:

  * Compute `interval` seconds between messages globally.
  * Log startup info.
  * While not shutting down, iterate tickers and produce messages.
  * If no quote available (rare), skip and sleep.

```python
                try:
                    meta = send_sync(KAFKA_TOPIC, payload)
                    log.debug("Sent %s -> partition=%s offset=%s", ticker, meta.partition, meta.offset)
                except Exception as e:
                    log.exception("Failed to send message for %s: %s", ticker, e)
```

* Synchronously send and log partition/offset on success; on failure log exception (stacktrace).

```python
                count += 1
                # flush periodically to avoid long queues (tunable)
                if count % max(1, int(1 / interval)) == 0:
                    producer.flush(timeout=5)
                time.sleep(interval)
```

* Periodic flush to ensure in-flight messages are delivered; then sleep to respect `MSGS_PER_SEC`.

```python
    except Exception:
        log.exception("Producer main loop crashed")
    finally:
        try:
            producer.flush(timeout=10)
            producer.close()
            log.info("Producer closed gracefully")
        except Exception:
            log.exception("Error closing producer")
```

* Global exception handling — logs crash and ensures producer flush/close in `finally`.

```python
if __name__ == "__main__":
    main()
```

* Standard entrypoint for script.

---

# 6) `spark/app.py` — detailed walkthrough (every line/block)

This is the most important file. I’ll break it into labeled sections and explain each line and the purpose.

```python
#!/usr/bin/env python3
"""
Spark Structured Streaming app for stock ticks:
- consumes JSON messages from Kafka
- writes: aggregated parquet + csv, raw parquet + csv (test)
- configurable via env variables
"""
```

* Shebang and module docstring summarizing behavior.

```python
import os
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg, min as spark_min, max as spark_max,
    first, last, date_format, current_timestamp, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
```

* Imports:

  * `os` for environment variables,
  * `signal` to implement graceful stops,
  * Spark session & functions used in streaming transformations (`from_json`, `window`, `avg`, `first`, `last`, etc.),
  * `StructType` / `StructField` for JSON schema.

```python
# ---------- Config (env-driven) ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
CHECKPOINT_BASE = os.path.join(OUTPUT_PATH, "_checkpoints")

# windows / triggers (tweakable)
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK = os.getenv("WATERMARK", "2 minutes")
PROCESSING_TRIGGER = os.getenv("PROCESSING_TRIGGER", "1 minute")  # micro-batch trigger

# raw sink test specifics
RAW_OUTPUT = os.path.join(OUTPUT_PATH, "raw_test")
RAW_CHECKPOINT = os.path.join(CHECKPOINT_BASE, "raw_test")

# parquet/csv outputs
PARQUET_OUTPUT = OUTPUT_PATH
PARQUET_CHECKPOINT = os.path.join(CHECKPOINT_BASE, "parquet")
CSV_OUTPUT = os.path.join(OUTPUT_PATH, "csv")
CSV_CHECKPOINT = os.path.join(CHECKPOINT_BASE, "csv")

# misc
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "spark-stock-group")
```

* Configuration section:

  * Reads environment variables with defaults. Centralizes all behavior tuning in envs so you can change behavior without editing code.
  * `CHECKPOINT_BASE` organizes checkpoint directories under `OUTPUT_PATH/_checkpoints`.
  * `WINDOW_DURATION`, `WATERMARK`, `PROCESSING_TRIGGER` control windowing behavior and micro-batch timing.
  * `MAX_OFFSETS_PER_TRIGGER` prevents reading too many messages per micro-batch (throttling).
  * `CONSUMER_GROUP` groups Spark Kafka consumers (useful for exclude/inspecting groups).

```python
# ---------- Spark session ----------
spark = SparkSession.builder.appName("stock-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
```

* Create Spark session for job and set Spark log level to WARN to reduce noise.

```python
_running = True

def shutdown(signum, frame):
    global _running
    _running = False
    print(f"Received {signum}, will stop streaming gracefully...")

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)
```

* Graceful shutdown handle:

  * `_running` flag is polled in `await_streams` to decide when to stop.
  * Signal handlers set `_running = False` so streaming loops stop gracefully.

```python
# ---------- schema ----------
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("ts", StringType(), True),
    StructField("ts_epoch_ms", LongType(), True),
])
```

* Defines expected JSON schema for incoming Kafka `value` (JSON string). Important so `from_json` can parse and create typed columns.

```python
def build_streams():
    # read from kafka
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")  # dev: earliest; prod: latest
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("kafka.group.id", CONSUMER_GROUP)
        .load()
    )
```

* `build_streams()` encapsulates building all streaming queries and returns them.
* `spark.readStream.format("kafka")` reads from Kafka.

  * `startingOffsets="earliest"` ensures that on start during development the job consumes from earliest offset so you see historic messages. In production use `latest` and rely on checkpoints.
  * `maxOffsetsPerTrigger` throttles per micro-batch.
  * `kafka.group.id` sets the consumer group identifier for Kafka.

```python
    # console raw (debug) - will print consumed JSON
    raw_console = (
        df_raw.selectExpr("CAST(value AS STRING) as json_str")
        .writeStream.format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )
```

* `raw_console` prints raw consumed messages (string) to Spark console logs (useful for debugging during development).

```python
    # parse JSON and add event_time
    df_parsed = (
        df_raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("ts")))
    )
```

* Parses the JSON string column into structured columns using the schema and creates `event_time` column by parsing the string `ts` to Spark `TimestampType` — necessary for time-windowed aggregations.

```python
    # test raw sinks (writes every consumed row) - handy to verify write permissions & mounts
    raw_to_write = df_parsed.withColumn("write_time", current_timestamp()) \
                            .select("symbol", "price", "volume", "ts", "ts_epoch_ms", "event_time", "write_time")
```

* Prepares a `raw_to_write` DataFrame that includes a `write_time` column to track when each record was written. This stream ensures write path works (Parquet/CSV) quickly.

```python
    raw_parquet_q = (
        raw_to_write.writeStream.format("parquet")
        .option("path", os.path.join(RAW_OUTPUT, "parquet"))
        .option("checkpointLocation", os.path.join(RAW_CHECKPOINT, "parquet"))
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
```

* `raw_parquet_q` writes raw messages to Parquet under `RAW_OUTPUT/parquet`. `checkpointLocation` ensures streaming progress is tracked. Trigger every 30s so files appear quickly.

```python
    raw_csv_q = (
        raw_to_write.writeStream.format("csv")
        .option("path", os.path.join(RAW_OUTPUT, "csv"))
        .option("checkpointLocation", os.path.join(RAW_CHECKPOINT, "csv"))
        .option("header", "true")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
```

* Same as above but CSV sink. CSV streaming sink creates multiple small files per micro-batch; good for quick inspection.

```python
    # dedupe
    df_dedup = df_parsed.dropDuplicates(["symbol", "ts_epoch_ms"])
```

* Deduplicates events using `symbol` + `ts_epoch_ms` composite key to avoid double counting if producer re-sent messages.

```python
    # aggregations: tumbling window per symbol
    agg = (
        df_dedup.withWatermark("event_time", WATERMARK)
        .groupBy(window(col("event_time"), WINDOW_DURATION), col("symbol"))
        .agg(
            avg(col("price")).alias("price_avg"),
            spark_min(col("price")).alias("price_min"),
            spark_max(col("price")).alias("price_max"),
            first(col("price")).alias("price_first"),
            last(col("price")).alias("price_last"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("price_avg"),
            col("price_min"),
            col("price_max"),
            col("price_first"),
            col("price_last"),
        )
    )
```

* Core aggregation logic:

  * `withWatermark("event_time", WATERMARK)` instructs Spark how long it should wait for late data (here `2 minutes`). Watermark is needed for state cleanup.
  * `groupBy(window(...), col("symbol"))` groups events per symbol in tumbling time windows (default 1 minute).
  * `agg(...)` computes aggregates: average price, min, max, first and last price inside the window.
  * `.select(...)` reformat and flatten columns to include `window_start` and `window_end` fields.

```python
    agg = agg.withColumn("pct_change", (col("price_last") - col("price_first")) / col("price_first") * 100)
    agg = agg.withColumn("date", date_format(col("window_start"), "yyyy-MM-dd"))
```

* Compute `pct_change` percent change inside the window and add a `date` column (string) for partitioning Parquet/CSV files, improving read performance and organization.

```python
    # parquet sink
    parquet_q = (
        agg.writeStream.format("parquet")
        .option("path", PARQUET_OUTPUT)
        .option("checkpointLocation", PARQUET_CHECKPOINT)
        .partitionBy("date")
        .outputMode("append")
        .trigger(processingTime=PROCESSING_TRIGGER)
        .start()
    )
```

* Writes aggregated results to Parquet:

  * `partitionBy("date")` stores results in `date=YYYY-MM-DD` directories.
  * `checkpointLocation` is required for streaming to track offsets and state.
  * `outputMode="append"` because aggregates are finalized each window.

```python
    # csv sink (streaming file sink)
    csv_q = (
        agg.writeStream.format("csv")
        .option("path", CSV_OUTPUT)
        .option("checkpointLocation", CSV_CHECKPOINT)
        .option("header", "true")
        .partitionBy("date")
        .outputMode("append")
        .trigger(processingTime=PROCESSING_TRIGGER)
        .start()
    )
```

* Same as parquet sink but writes CSV for easy inspection.

```python
    # console sink for aggregated results (debug)
    agg_console = (
        agg.writeStream.format("console")
        .option("truncate", "false")
        .outputMode("update")
        .start()
    )
```

* Console sink prints aggregated windows to logs; `outputMode="update"` prints updated rows when new results arrive.

```python
    # return all active queries so caller can await them
    return [raw_console, raw_parquet_q, raw_csv_q, parquet_q, csv_q, agg_console]
```

* Return list of active streaming queries for central management and graceful shutdown.

```python
def await_streams(queries):
    """Wait for all queries; stop when shutdown requested."""
    try:
        while _running:
            for q in queries:
                # break early if any stream terminated with exception
                if q.isActive is False:
                    raise RuntimeError(f"Stream {q.id} is not active")
            # sleep a short while (avoid busy loop)
            import time
            time.sleep(1)
    except Exception as ex:
        print("Streaming stopped due to:", ex)
    finally:
        print("Stopping streams...")
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
        print("Stopped.")
```

* `await_streams()` polls active queries and stops them gracefully when `_running` is false or if any stream becomes inactive. `q.stop()` triggers a graceful stop and avoids abrupt cancellation.

```python
def main():
    queries = build_streams()
    print("Started streams:", [q.id for q in queries])
    await_streams(queries)
```

* `main()` builds and awaits streams.

```python
if __name__ == "__main__":
    main()
```

* Standard entrypoint.

---

# 7) Workflow recap — step-by-step with code mapping

1. **Startup (docker-compose)**:

   * `docker-compose up` spins up services in order: `zookeeper` → `kafka` → `kafka-init` → `spark-master` & `spark-worker` → `spark-streaming` → `producer`.
   * `kafka-init` creates the topic `events` (3 partitions) before producer starts.

2. **Producer**:

   * `producer` container runs `producer.py`:

     * constructs payloads via `fetch_quote()` (live via yfinance or simulated),
     * builds JSON payload with `ts` and `ts_epoch_ms`,
     * calls `producer.send()` synchronously and optionally flushes periodically.
   * Messages published to Kafka topic `events`.

3. **Kafka**:

   * Stores messages in topic partitions; `spark-streaming` consumer group reads from these partitions.

4. **Spark streaming job (`spark/app.py`)**:

   * `spark.readStream` connects to Kafka (bootstrap `kafka:9092`, `startingOffsets=earliest` during dev).
   * Raw console sink prints JSON strings so you can verify messages are consumed.
   * `df_parsed` parses JSON into typed columns and builds `event_time`.
   * `raw_test` sinks write raw Parquet/CSV for initial verification of write path.
   * `df_dedup` removes duplicate events using `symbol+ts_epoch_ms`.
   * Aggregation window groups by symbol and 1-minute time window:

     * `avg`, `min`, `max`, `first`, `last` prices computed.
     * `pct_change` computed from first/last.
   * Writes aggregated results to Parquet and CSV partitioned by `date`.
   * Checkpoints stored under `OUTPUT_PATH/_checkpoints/*` so Spark can resume processing and track offsets.

---

# 8) Important details, failure points and tuning advice

* **startingOffsets**:

  * `earliest` is great for dev to reprocess existing messages; in production use `latest` and rely on checkpoints to resume.
* **Checkpoint directories**:

  * Must be unique per sink and persist across restarts. If you delete checkpoints, Spark may reprocess older messages (duplicates).
* **Watermark**:

  * Determines when Spark considers late events too old and cleans up state. `2 minutes` is fine for low-latency pipelines; adjust depending on expected delays.
* **maxOffsetsPerTrigger**:

  * Tune to control ingestion rate per micro-batch (throttle).
* **File sinks vs. CDC/Delta**:

  * Parquet files are append-only; consider Delta Lake or Iceberg for ACID/upserts if you need more advanced semantics.
* **Producer synchronous sends**:

  * Good for dev to surface issues. In production, consider async with callbacks for throughput.
* **Permissions**:

  * Ensure host `./data/output` is writeable by containers (you used `chmod -R 777` earlier for dev).
* **Kafka advertised listeners**:

  * If external tools need to connect to Kafka from host, `PLAINTEXT_HOST://localhost:9094` mapping is required. For container-to-container, `kafka:9092` is correct.
* **Spark resource constraints**:

  * On local machines, limit `SPARK_WORKER_MEMORY` to avoid OOM. You used `2g` in compose — adjust for your machine.
