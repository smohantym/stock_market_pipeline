#!/usr/bin/env python3
"""
Spark Structured Streaming app for stock ticks:
- consumes JSON messages from Kafka
- writes: aggregated parquet + csv, raw parquet + csv (test)
- configurable via env variables
"""
import os
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg, min as spark_min, max as spark_max,
    first, last, date_format, current_timestamp, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

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

# ---------- Spark session ----------
spark = SparkSession.builder.appName("stock-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# make a clean shutdown on SIGTERM
_running = True


def shutdown(signum, frame):
    global _running
    _running = False
    print(f"Received {signum}, will stop streaming gracefully...")


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ---------- schema ----------
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("ts", StringType(), True),
    StructField("ts_epoch_ms", LongType(), True),
])


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

    # console raw (debug) - will print consumed JSON
    raw_console = (
        df_raw.selectExpr("CAST(value AS STRING) as json_str")
        .writeStream.format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )

    # parse JSON and add event_time
    df_parsed = (
        df_raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("ts")))
    )

    # test raw sinks (writes every consumed row) - handy to verify write permissions & mounts
    raw_to_write = df_parsed.withColumn("write_time", current_timestamp()) \
                            .select("symbol", "price", "volume", "ts", "ts_epoch_ms", "event_time", "write_time")

    raw_parquet_q = (
        raw_to_write.writeStream.format("parquet")
        .option("path", os.path.join(RAW_OUTPUT, "parquet"))
        .option("checkpointLocation", os.path.join(RAW_CHECKPOINT, "parquet"))
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    raw_csv_q = (
        raw_to_write.writeStream.format("csv")
        .option("path", os.path.join(RAW_OUTPUT, "csv"))
        .option("checkpointLocation", os.path.join(RAW_CHECKPOINT, "csv"))
        .option("header", "true")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # dedupe
    df_dedup = df_parsed.dropDuplicates(["symbol", "ts_epoch_ms"])

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

    agg = agg.withColumn("pct_change", (col("price_last") - col("price_first")) / col("price_first") * 100)
    agg = agg.withColumn("date", date_format(col("window_start"), "yyyy-MM-dd"))

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

    # console sink for aggregated results (debug)
    agg_console = (
        agg.writeStream.format("console")
        .option("truncate", "false")
        .outputMode("update")
        .start()
    )

    # return all active queries so caller can await them
    return [raw_console, raw_parquet_q, raw_csv_q, parquet_q, csv_q, agg_console]


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


def main():
    queries = build_streams()
    print("Started streams:", [q.id for q in queries])
    await_streams(queries)


if __name__ == "__main__":
    main()
