#!/usr/bin/env python3
"""
Robust Kafka producer for stock tick events.
- Uses yfinance to fetch quotes (falls back to simulation).
- Sends synchronously (fut.get(timeout)) to surface delivery errors.
- Graceful SIGINT/SIGTERM shutdown.
- Configurable via environment variables.
"""
import os
import time
import json
import signal
import logging
from datetime import datetime, timezone
import random

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Optional heavy deps; import lazily to keep startup fast when using simulated mode
try:
    import yfinance as yf  # type: ignore
except Exception:
    yf = None  # will fallback to simulate if needed

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

# config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
MSGS_PER_SEC = float(os.getenv("MSGS_PER_SEC", "5"))
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "AAPL,MSFT,GOOG,AMZN,TSLA").split(",")]
SIMULATE_IF_NO_YFINANCE = os.getenv("SIMULATE_IF_NO_YFINANCE", "true").lower() in ("1", "true", "yes")
SEND_TIMEOUT = float(os.getenv("SEND_TIMEOUT_SEC", "10"))

# producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    linger_ms=10,
)

_shutting_down = False


def signal_handler(signum, frame):
    global _shutting_down
    log.info("Received signal %s, shutting down...", signum)
    _shutting_down = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


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


def fetch_quote_simulated(ticker: str):
    base = random.uniform(100, 1000)
    return {"price": round(base + random.uniform(-1, 1), 2), "volume": int(random.uniform(100, 10000))}


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


def build_payload(ticker: str, quote: dict):
    ts = datetime.now(timezone.utc)
    return {
        "symbol": ticker,
        "price": float(quote["price"]),
        "volume": int(quote["volume"]) if quote.get("volume") is not None else None,
        "ts": ts.isoformat(),
        "ts_epoch_ms": int(ts.timestamp() * 1000),
    }


def send_sync(topic: str, payload: dict, timeout: float = SEND_TIMEOUT):
    """Send synchronously and return metadata or raise."""
    try:
        fut = producer.send(topic, value=payload)
        meta = fut.get(timeout=timeout)
        return meta
    except KafkaError as e:
        raise


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

                # synchronous send to surface delivery issues during dev
                try:
                    meta = send_sync(KAFKA_TOPIC, payload)
                    log.debug("Sent %s -> partition=%s offset=%s", ticker, meta.partition, meta.offset)
                except Exception as e:
                    log.exception("Failed to send message for %s: %s", ticker, e)

                count += 1
                # flush periodically to avoid long queues (tunable)
                if count % max(1, int(1 / interval)) == 0:
                    producer.flush(timeout=5)

                time.sleep(interval)

    except Exception:
        log.exception("Producer main loop crashed")
    finally:
        try:
            producer.flush(timeout=10)
            producer.close()
            log.info("Producer closed gracefully")
        except Exception:
            log.exception("Error closing producer")


if __name__ == "__main__":
    main()
