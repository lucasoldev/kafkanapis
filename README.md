# Kafka n APIs

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6+-231F20.svg)](https://kafka.apache.org/)

**Streaming Pi-hole DNS events, Public API data, and Localhost Random Data through Kafka**

`Kafka n APIs` ingests data from three independent sources:
- **Pi-hole DNS logs** (via local file + API)
- **Public APIs** (geolocation, threat intelligence, etc.)
- **Localhost random data generator API** (for testing, simulation, and integration)

All sources are published into Apache Kafka topics, where downstream consumers can process, correlate, and act on these event streams independently.

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

`Kafka n APIs` treats everything as an event stream. Three independent producers feed data into Kafka:

### 1. **Pi-hole DNS logs** – with dual ingestion strategy:
   - **File monitoring**: Watches `/var/log/pihole/pihole.log` in real-time using a tail-like approach, ideal for local access.
   - **API polling**: Queries `/api/logs/dnsmasq` (or `/api/logs/ftl`) periodically or on-demand, ideal for remote access.

### 2. **Public APIs** – external data (geolocation, threat intelligence, domain reputation, etc.)

### 3. **Localhost Random API** – synthetic data for testing, correlation, and simulation.

From there, independent consumer services subscribe to these topics and process the data in real-time.

This design decouples data sources from processing logic, making it easy to add new APIs, swap consumers, or scale parts of the system independently.

---

## Architecture

```
┌──────────────────────────┐     ┌────────────────────────────────────────┐
│    Pi-hole (local)      │────▶│                                        │
│  /var/log/pihole.log    │     │                                        │
└──────────────────────────┘     │                                        │
                                 │              Apache Kafka              │
┌──────────────────────────┐     │                                        │
│    Pi-hole (API)        │────▶│                                        │
│  /api/logs/dnsmasq      │     │                                        │
└──────────────────────────┘     │                                        │
                                 │                                        │
┌──────────────────────────┐     │                                        │
│    Public APIs           │────▶│                                        │
│  (multiple)              │     │                                        │
└──────────────────────────┘     └────────────┬──────────────┬────────────┘
                                 │              │
┌──────────────────────────┐     │              │
│    Localhost Random API  │────▶│              │
└──────────────────────────┘     │              │
                                 │              │
                          ┌───────▼───┐  ┌───────▼───┐
                          │ Consumer  │  │ Consumer  │
                          │  Service  │  │  Service  │
                          └───────────┘  └───────────┘
```

1. **Producers**
   - `Pi-hole file producer` → tailing `pihole.log`
   - `Pi-hole API producer` → polling `/api/logs/dnsmasq`
   - `Public API producer` → various external APIs
   - `Random API producer` → localhost generator

2. **Broker** → Kafka cluster receiving, storing, and distributing all event streams.

3. **Consumers** → Independent services subscribing to topics, processing and correlating data from multiple sources.

---

## Features

- ✅ Pi-hole DNS logs via **local file monitoring** (real-time)
- ✅ Pi-hole DNS logs via **API polling** (remote access)
- ✅ Multiple public APIs fetched and published as Kafka events
- ✅ Localhost random data generator API for testing and simulation
- ✅ Independent consumer services processing different topics
- ✅ Correlation between DNS events, enriched API data, and synthetic patterns
- ✅ Configurable topics, consumer groups, and partitioning
- ✅ Designed for local development with Docker Compose

---

## Tech Stack

| Layer          | Technology                          |
|----------------|-------------------------------------|
| **Messaging**  | Apache Kafka                        |
| **Producers**  | Python + `kafka-python`             |
| **Consumers**  | Python microservices                |
| **HTTP**       | `requests`                          |
| **APIs**       | Pi-hole, ip-api.com, viacep.com.br, Localhost Random API |
| **Data**       | `pandas`                            |
| **Database**   | PostgreSQL + `psycopg2-binary`      |
| **Config**     | `python-dotenv`                     |
| **Containers** | Docker + Docker Compose             |
| **Dev tools**  | `venv`, `Flask` (for localhost API) |

---

## Getting Started

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- Pi-hole instance accessible on your network (either local or remote)

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

Edit `.env` with your Pi-hole URL, API tokens, Kafka bootstrap server, and localhost API settings.

### 5. Run the localhost random API (separate terminal)

```bash
python -m producer.random_api_server
```

> Runs a Flask server at `http://localhost:5000/random`

### 6. Run the producers

**Pi-hole (file monitor)** – watch local log file:
```bash
python -m producer.pi_hole_file_monitor
```

**Pi-hole (API poller)** – fetch logs via API:
```bash
python -m producer.pi_hole_api_poller
```

**Public APIs:**
```bash
python -m producer.api_fetcher
```

**Random API:**
```bash
python -m producer.random_api_fetcher
```

### 7. Run a consumer

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
│   ├── pi_hole_file_monitor.py   # Tailing pihole.log → Kafka
│   ├── pi_hole_api_poller.py     # Fetching /api/logs/dnsmasq → Kafka
│   ├── api_fetcher.py            # Public APIs → Kafka
│   └── random_api_fetcher.py     # Localhost random API → Kafka
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
| `PIHOLE_LOG_PATH`     | Path to local pihole.log        | `/var/log/pihole/pihole.log` |
| `RANDOM_API_URL`      | Localhost random API URL        | `http://localhost:5000/random` |
| `RANDOM_API_INTERVAL` | Seconds between API calls       | `5`               |
| `DNS_LOGS_TOPIC`      | Kafka topic for DNS logs        | `pi-hole.dns.raw` |
| `API_DATA_TOPIC`      | Kafka topic for public API data | `public.api.data` |
| `RANDOM_DATA_TOPIC`   | Kafka topic for random data     | `random.data.raw` |

---

## Usage

**Monitor Pi-hole logs locally (file):**

```bash
python -m producer.pi_hole_file_monitor
```

**Fetch Pi-hole logs via API (remote):**

```bash
python -m producer.pi_hole_api_poller
```

**Produce public API data:**

```bash
python -m producer.api_fetcher
```

**Produce random data from localhost API:**

```bash
python -m producer.random_api_fetcher
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
- [ ] Correlation engine between Pi-hole and random data
- [ ] Localhost API with configurable schemas
- [ ] **Pi-hole log file monitoring** (✅ implemented)
- [ ] **Pi-hole log API polling** (✅ implemented)

---

## 📍 **Notas sobre a implementação dos dois métodos**

### **1. Monitoramento de arquivo local (`pi_hole_file_monitor.py`)**

Este produtor usa `tail -f` (ou equivalente em Python com `subprocess`) para acompanhar o arquivo `/var/log/pihole/pihole.log` em tempo real. Cada nova linha do log é parseada, transformada em JSON e enviada para o Kafka.

**Vantagens:** Baixa latência, detecção imediata de novos eventos.

**Limitações:** Requer acesso SSH ou local ao Pi-hole.

### **2. Polling via API (`pi_hole_api_poller.py`)**

Este produtor faz requisições periódicas (ex: a cada 5 segundos) ao endpoint `/api/logs/dnsmasq` para obter as últimas N linhas do log. Cada resposta é processada e enviada ao Kafka.

**Vantagens:** Acesso remoto, não precisa de SSH.

**Limitações:** Latência adicional (devido ao intervalo de polling), pode não capturar eventos entre as consultas.

