FROM python:3.11-slim

ARG DEBIAN_FRONTEND=noninteractive
ENV DEBIAN_FRONTEND=${DEBIAN_FRONTEND}
WORKDIR /app

# Keep apt minimal and non-interactive; install essential tools only
RUN set -eux; \
    apt-get update -y || (sleep 2 && apt-get update -y); \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        wget \
    ; \
    rm -rf /var/lib/apt/lists/*

# Ensure pip can select manylinux wheels
RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt .

# prefer binary wheels to avoid costly builds on aarch64
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

COPY producer.py .

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=events
ENV TICKERS=AAPL,MSFT,GOOG,AMZN,TSLA
ENV MSGS_PER_SEC=5

CMD ["python", "producer.py"]
