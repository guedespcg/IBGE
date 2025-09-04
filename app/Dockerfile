FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# >>> OpenSSL config para renegociação legada <<<
ENV OPENSSL_CONF=/etc/ssl/openssl_ibge_legacy.cnf
COPY openssl_ibge_legacy.cnf /etc/ssl/openssl_ibge_legacy.cnf

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY . /app
