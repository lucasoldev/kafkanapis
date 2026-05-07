```
# Kafka n APIs

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6+-231F20.svg)](https://kafka.apache.org/)

**Streaming Pi-hole DNS events and Public API data through Kafka**

`Kafka n APIs` ingests data from [Pi-hole](https://pi-hole.net/) and multiple public APIs into Apache Kafka topics, where downstream consumers process, correlate, and act on these event streams independently.

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

`Kafka n APIs` treats everything as an event stream. DNS query logs from Pi-hole and data fetched from public APIs (geolocation, threat intelligence, domain reputation, etc.) are both published as producers into Kafka topics. From there, independent consumer services subscribe to these topics and process the data in real-time.

This design decouples data sources from processing logic, making it easy to add new APIs, swap consumers, or scale parts of the system independently — exactly what Kafka and microservices are meant for.

---

## Architecture

```
┌──────────────┐     ┌────────────────────────────────────────┐
│   Pi-hole    │────▶│                                        │
│  DNS logs    │     │                                        │
└──────────────┘     │                                        │
                     │              Apache Kafka              │
┌──────────────┐     │                                        │
│  Public APIs │────▶│                                        │
│  (multiple)  │     │                                        │
└──────────────┘     └────────────┬──────────────┬────────────┘
                                  │              │
                          ┌───────▼───┐  ┌───────▼───┐
                          │ Consumer  │  │ Consumer  │
                          │  Service  │  │  Service  │
                          └───────────┘  └───────────┘
```

1. **Producers** — Pi-hole logs and public API data are both published into Kafka topics.
2. **Broker** — Kafka cluster receiving, storing, and distributing all event streams.
3. **Consumers** — Independent services subscribing to topics, processing and correlating data from multiple sources.

---

## Features

- ✅ Pi-hole DNS queries ingested into Kafka in real-time
- ✅ Multiple public APIs fetched and published as Kafka events
- ✅ Independent consumer services processing different topics
- ✅ Correlation between DNS events and enriched API data
- ✅ Configurable topics, consumer groups, and partitioning
- ✅ Designed for local development with Docker Compose

---

## Tech Stack

| Layer          | Technology                          |
|----------------|-------------------------------------|
| **Messaging**  | Apache Kafka                        |
| **Producers**  | Python + `kafka-python` / `confluent-kafka` |
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
git clone https://github.com/SEU_USUARIO/kafka-n-apis.git
cd kafka-n-apis
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

### 5. Run the producers

```bash
python -m producer.pi_hole_ingest
python -m producer.api_fetcher
```

### 6. Run a consumer

```bash
python -m consumers.event_processor
```

---

## Project Structure

```
kafka-n-apis/
├── docker-compose.yml          # Kafka + Zookeeper
├── .env.example                # Environment template
├── requirements.txt
├── README.md
├── producer/
│   ├── pi_hole_ingest.py       # Pi-hole DNS logs → Kafka
│   └── api_fetcher.py          # Public APIs → Kafka
├── consumers/
│   ├── event_processor.py      # Process and correlate events
│   └── alert_service.py        # Alerting based on patterns
├── services/
│   └── api_client.py           # Shared API client wrapper
├── schemas/
│   └── events.py               # Event schema definitions
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
| `DNS_QUERIES_TOPIC`   | Kafka topic for DNS queries     | `pi-hole.dns.raw` |
| `API_DATA_TOPIC`      | Kafka topic for API data        | `public.api.data` |

---

## Usage

**Produce Pi-hole events:**

```bash
python -m producer.pi_hole_ingest
```

**Produce public API data:**

```bash
python -m producer.api_fetcher
```

**Run a consumer:**

```bash
python -m consumers.event_processor
```

**List topics and consumer groups:**

```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Roadmap

- [ ] WebSocket API for real-time dashboards
- [ ] Dead-letter queue for failed events
- [ ] Schema Registry and Avro support
- [ ] Metrics export (Prometheus)
- [ ] Kubernetes manifests
```