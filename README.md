# Stock Market Streaming Pipeline

**Stock Market Streaming Pipeline** — a self-contained dev project using **Kafka + Spark Structured Streaming** to ingest (live or simulated) stock quotes, stream them through Kafka, and process & persist results with Spark.

## Project structure
```
.
├── docker-compose.yml            # orchestrates zookeeper, kafka, spark cluster, producer
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py                # fetches quotes (yfinance or simulated) and publishes to Kafka
├── spark/
│   └── app.py                     # Spark Structured Streaming job (consumes, aggregates, writes parquet/csv)
├── data/
│   └── output/                    # host-mounted output (Parquet / CSV written here by Spark)
└── README.md
```

---

## High-level workflow

1. **Producer** container runs `producer.py`:

   * fetches quotes (via `yfinance` when available, otherwise simulated),
   * constructs JSON messages:

     ```json
     {
       "symbol": "AAPL",
       "price": 569.01,
       "volume": 644,
       "ts": "2025-11-12T14:38:30.294758+00:00",
       "ts_epoch_ms": 1762958310294
     }
     ```
   * publishes messages to Kafka topic `events`.

2. **Kafka** (Confluent) stores messages in the `events` topic.

3. **Spark Structured Streaming** (`spark/app.py`) consumes topic:

   * parses JSON into typed columns,
   * deduplicates events,
   * computes 1-minute tumbling-window aggregates per symbol (avg/min/max/first/last + pct_change),
   * writes aggregated results to **Parquet** and **CSV** (partitioned by date),
   * writes **raw** Parquet/CSV for quick verification,
   * includes console sinks for debugging.

4. Output files are written to `./data/output` on the host (mounted into Spark containers).

---

## Quick start (dev) — run everything with Docker Compose

> Requirements: Docker & Docker Compose (v2). The repo already has an ARM-friendly Confluent Kafka image; adjust images if needed for other platforms.

From the project root:

```bash
# 1) Ensure output folder is writable
mkdir -p ./data/output
chmod -R 777 ./data/output

# 2) Build & start all services (first run)
docker-compose up --build

# or run detached:
docker-compose up --build -d

# 3) Tail logs (example)
docker-compose logs -f producer spark-streaming kafka
```

Stop & cleanup:

```bash
# stop containers
docker-compose down

# remove volumes (warning: deletes kafka data)
docker-compose down --volumes --remove-orphans
```

---

## Inspecting output

Spark writes files to the host-mounted `./data/output` directory.

Typical output layout:

```
./data/output/
├─ parquet/                # Aggregated parquet (partitioned by date) + raw_test/parquet
│  ├─ date=2025-11-12/...
│  └─ raw_test/parquet/...
├─ csv/                    # Aggregated CSVs (partitioned by date)
│  └─ date=2025-11-12/...
└─ parquet/raw_test/csv    # raw CSV fragments for consumed messages (debug)
```

You can explore files on the host:

```bash
# list recent files
ls -R ./data/output | sed -n '1,200p'

# read a Parquet file (if you have pyarrow/pandas locally)
python - <<PY
import pyarrow.parquet as pq
import sys
p = './data/output/parquet/date=2025-11-12/part-00000-*.parquet'  # example
print("Use your parquet reader or Spark to inspect files")
PY
```

Or inspect inside the `spark-streaming` container:

```bash
docker exec -it spark-streaming ls -R /opt/spark-output | head
```

---

## Configuration & environment variables

You can tweak behavior via env vars (set in `docker-compose.yml` or override with a `.env` or `docker-compose -f docker-compose.yml up -d` plus environment):

### Producer (env in `docker-compose.yml` or Dockerfile defaults)

* `KAFKA_BOOTSTRAP_SERVERS` — Kafka bootstrap address (default `kafka:9092`)
* `KAFKA_TOPIC` — Kafka topic (default `events`)
* `MSGS_PER_SEC` — approximate message generation rate (default `5`)
* `TICKERS` — comma-separated tickers (default `AAPL,MSFT,GOOG,AMZN,TSLA`)
* `SIMULATE_IF_NO_YFINANCE` — fallback to simulated if yfinance not available (`true`)

### Spark (env in `docker-compose.yml` or app uses defaults)

* `KAFKA_BOOTSTRAP_SERVERS` — Kafka bootstrap (default `kafka:9092`)
* `KAFKA_TOPIC` — topic to subscribe to (default `events`)
* `OUTPUT_PATH` — base output path inside container (default `/opt/spark-output/parquet`)
* `WINDOW_DURATION` — window size for aggregation (default `1 minute`)
* `WATERMARK` — watermark for late data (default `2 minutes`)
* `PROCESSING_TRIGGER` — micro-batch trigger (default `1 minute`)
* `MAX_OFFSETS_PER_TRIGGER` — throttle rate (default `10000`)
* `KAFKA_CONSUMER_GROUP` — consumer group id for Spark (default `spark-stock-group`)

---

## Files explained (short)

* `docker-compose.yml` — all services: zookeeper, kafka, kafka-init (creates topic), spark-master, spark-worker, spark-streaming, producer. Top-level `volumes:` defines `kafka-data` for persistence.
* `producer/Dockerfile` — builds producer image, installs Python deps.
* `producer/requirements.txt` — pinned Python libs (`kafka-python`, `yfinance`, `pandas`, `numpy`).
* `producer/producer.py` — robust script producing JSON messages to Kafka (with graceful shutdown and optional simulation fallback).
* `spark/app.py` — Spark Structured Streaming job (consumes Kafka, parses JSON, aggregates, writes parquet & csv, raw test sinks & console sinks).

---

## Troubleshooting & common commands

### Check Kafka topics

```bash
docker exec -it kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list
docker exec -it kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic events
```

### Consume from topic manually

```bash
docker exec -it kafka /usr/bin/kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events \
  --from-beginning \
  --timeout-ms 10000
```

### Inspect Spark logs

```bash
docker logs -f spark-streaming
```

Look for:

* JSON strings printed by the raw console sink (indicates Spark is consuming).
* Any exceptions or errors about writing to disk, permissions, or missing Kafka connectors.

### Mount / permission issues

If Spark cannot write files, ensure host dir exists and is writable:

```bash
mkdir -p ./data/output
chmod -R 777 ./data/output
```

### If Spark doesn't see earlier messages

* `spark/app.py` uses `startingOffsets="earliest"` in dev. If set to `latest` and Spark starts after producer produced events, it won't read previous messages. For dev, use `earliest`.

### Kafka advertised listeners

* If you plan to use host tools to connect to this Kafka broker, use `localhost:9094` as configured in `docker-compose.yml`. Container-internal clients use `kafka:9092`.
