# kafkanapis

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6+-231F20.svg)](https://kafka.apache.org/)

**Kafka-powered pipeline: Pi-hole DNS events + Public API data**

`kafkanapis` is a microservices-based application that streams DNS query logs from [Pi-hole](https://pi-hole.net/) through Apache Kafka, enriching the data with public APIs for real-time network observability and analytics.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Usage](#usage)
- [Roadmap](#roadmap)

---

## Overview

Pi-hole generates a continuous stream of DNS queries across your network. `kafkanapis` captures this firehose of data, pipes it through Kafka topics, and cross-references it with public APIs — turning raw DNS logs into structured, queryable, and actionable information.

Whether you're monitoring suspicious domains, analyzing traffic patterns, or simply taming your home lab data, `kafkanapis` gives you a scalable event-driven foundation.

---

## Architecture


```
┌──────────────┐     ┌────────────┐     ┌───────────────┐
│   Pi-hole    │────▶│   Kafka    │────▶│  Consumers    │
│  DNS logs    │     │  Cluster   │     │  (services)   │
└──────────────┘     └────────────┘     └───────┬───────┘
                                                │
                                        ┌───────▼───────┐
                                        │  Public APIs  │
                                        │  (enrichment) │
                                        └───────────────┘
```

1. **Producer** — Reads Pi-hole query logs and publishes raw events to a Kafka topic.
2. **Broker** — Kafka cluster (local or cloud) handling message distribution.
3. **Consumers** — Independent services consuming events, calling public APIs for domain lookups, geolocation, threat intelligence, etc.
4. **API Gateway** — (optional) Exposes enriched results via REST or WebSocket.

---

## Features

- ✅ Real-time ingestion of Pi-hole DNS queries into Kafka
- ✅ Microservices consumers processing events independently
- ✅ Integration with public APIs (IP geolocation, domain reputation, ASN lookup)
- ✅ Schema-registered events for type safety
- ✅ Configurable topics, consumer groups, and partitioning
- ✅ Designed for local development with Docker Compose

---

## Tech Stack

| Layer          | Technology                          |
|----------------|-------------------------------------|
| **Messaging**  | Apache Kafka                        |
| **Producer**   | Python + `kafka-python` / `confluent-kafka` |
| **Consumers**  | Python microservices                |
| **APIs**       | ip-api.com, isc.org, others         |
| **Containers** | Docker + Docker Compose             |
| **Dev tools**  | `venv`, `pytest`, `black`, `ruff`   |

---

## Getting Started

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Pi-hole instance accessible on your network

### 1. Clone the repository

```bash
git clone https://github.com/SEU_USUARIO/kafkanapis.git
cd kafkanapis
```

### 2. Start Kafka and Zookeeper

```bash
docker compose up -d
```

> Uses `bitnami/kafka` and `bitnami/zookeeper`. Brokers available at `localhost:9092`.

### 3. Install Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

pip install -r requirements.txt
```

### 4. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your Pi-hole URL, API tokens, and Kafka bootstrap server.

### 5. Run the producer

```bash
python -m producer.pi_hole_ingest
```

### 6. Run a consumer

```bash
python -m consumers.ip_enricher
```

---

## Project Structure

```
kafkanapis/
├── docker-compose.yml          # Kafka + Zookeeper
├── .env.example                # Environment template
├── requirements.txt
├── README.md
├── producer/
│   └── pi_hole_ingest.py       # Reads Pi-hole → Kafka
├── consumers/
│   ├── ip_enricher.py          # IP geolocation via public API
│   └── domain_reputation.py    # Threat intel lookups
├── services/
│   └── api_client.py           # Shared API client wrapper
├── schemas/
│   └── dns_event.py            # Event schema definition
├── tests/
│   └── test_producer.py
└── scripts/
    └── create_topics.sh
```

---

## Configuration

| Variable              | Description                     | Default           |
|-----------------------|---------------------------------|-------------------|
| `KAFKA_BOOTSTRAP`     | Kafka bootstrap server          | `localhost:9092`  |
| `PIHOLE_URL`          | Pi-hole admin API URL           | —                 |
| `PIHOLE_API_TOKEN`    | Pi-hole API token               | —                 |
| `DNS_QUERIES_TOPIC`   | Kafka topic for raw DNS queries | `pi-hole.dns.raw` |

---

## Usage

**Produce DNS events:**

```bash
python -m producer.pi_hole_ingest
```

**Consume and enrich with IP geolocation:**

```bash
python -m consumers.ip_enricher
```

**List topics and consumer groups:**

```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Roadmap

- [ ] WebSocket API for real-time dashboards
- [ ] Dead-letter queue for failed enrichments
- [ ] Schema Registry and Avro support
- [ ] Metrics export (Prometheus)
- [ ] Kubernetes manifests