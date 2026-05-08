## 📝 **Atualização final do README**

# Kafka n APIs

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6+-231F20.svg)](https://kafka.apache.org/)

**Streaming Pi-hole DNS events, Public API data, and Localhost Random Data through Kafka**

`Kafka n APIs` ingests data from **five independent sources** into Apache Kafka topics, where **three independent consumers** process, correlate, and act on these event streams.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Usage](#usage)
- [Roadmap](#roadmap)

---

## Overview

`Kafka n APIs` treats everything as an event stream. Data flows from **five independent sources** into Kafka topics:

| Source | Description |
|--------|-------------|
| **Pi-hole (local)** | Tails `/var/log/pihole/pihole.log` |
| **Pi-hole (API logs)** | Polls `/api/logs/dnsmasq` |
| **Pi-hole (API)** | Fetches `/devices`, `/top_clients`, `/upstreams`, `/ftl`, `/system`, `/queries` |
| **Public APIs** | External data from multiple free test APIs |
| **Localhost Random API** | Synthetic data (people, companies, random text) via `Faker` + Flask |

From there, **three independent consumers** subscribe to these topics and process the data:

1. **Consumer 1** – processes DNS logs (local + API)
2. **Consumer 2** – processes metrics and system data
3. **Consumer 3** – processes external and synthetic data

---

## Architecture

```mermaid
flowchart TD
    %% Internet Cloud
    subgraph Internet [🌐 Internet]
        direction TB
        API1[Public APIs<br/>ip-api.com, viacep.com.br, etc.]
    end

    %% Local Network (LAN)
    subgraph LocalNetwork [🏠 Local Network - LAN]
        direction TB
        
        subgraph PiHole [🖥️ Pi-hole Server]
            direction TB
            P1[Local File<br/>/var/log/pihole.log]
            P2[API Endpoints<br/>/api/logs/dnsmasq<br/>/devices, /top_clients, /upstreams<br/>/ftl, /system, /queries]
        end

        subgraph PythonApp [🐍 Python Application]
            direction TB
            
            subgraph Producers [Producers]
                PF1[Pi-hole File Monitor]
                PP1[Pi-hole API Logs Poller]
                PP2[Pi-hole Data Poller]
                PF2[Public API Fetcher]
                PF3[Random API Fetcher]
            end

            subgraph Consumers [Consumers]
                C1[Consumer 1 - Logs]
                C2[Consumer 2 - Metrics]
                C3[Consumer 3 - External]
            end
            
            subgraph Services [Internal Services]
                S1[Random API Server<br/>Flask + Faker]
                S2[Kafka Client]
            end
            
            subgraph Models [Data Models]
                M1[Schemas & Validation]
            end
        end

        subgraph Kafka [📦 Kafka Cluster]
            K1[Broker 1<br/>localhost:9092]
            K2[Zookeeper]
        end

        subgraph Database [🗄️ PostgreSQL]
            DB1[Database]
        end
    end

    %% Component Connections
    API1 -->|HTTP| PF2
    PF2 -->|Produces| K1
    
    P1 -->|Reads| PF1
    PF1 -->|Produces| K1
    
    P2 -->|API| PP1
    PP1 -->|Produces| K1
    P2 -->|API| PP2
    PP2 -->|Produces| K1
    
    S1 -->|Generates| PF3
    PF3 -->|Produces| K1

    K1 -->|Consumes| C1
    K1 -->|Consumes| C2
    K1 -->|Consumes| C3

    C1 -->|Persists| DB1
    C2 -->|Persists| DB1
    C3 -->|Persists| DB1

    %% Styles
    classDef internet fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef lan fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef pihole fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef python fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef kafka fill:#fff8e1,stroke:#bf360c,stroke-width:2px
    classDef db fill:#e0f2f1,stroke:#004d40,stroke-width:2px

    class Internet internet
    class LocalNetwork lan
    class PiHole pihole
    class PythonApp python
    class Kafka kafka
    class Database db

```

**Legend:**

| Color | Component | Description |
|-------|-----------|-------------|
| 🔵 Light Blue | 🌐 Internet | External public APIs (HTTP access) |
| 🟣 Light Purple | 🏠 Local Network | Internal local network environment |
| 🟡 Light Orange | 🖥️ Pi-hole Server | Pi-hole server with local logs and API |
| 🟢 Light Green | 🐍 Python Application | Python code with producers, consumers, and services |
| 🟡 Light Yellow | 📦 Kafka Cluster | Kafka cluster for event streaming |
| 🟢 Teal | 🗄️ PostgreSQL | Database for persistence |


```
┌──────────────────────────┐     ┌────────────────────────────────────────┐
│    Pi-hole (local)       │────▶│                                        │
│  /var/log/pihole.log     │     │                                        │
└──────────────────────────┘     │                                        │
                                 │              Apache Kafka              │
┌──────────────────────────┐     │                                        │
│    Pi-hole (API logs)    │────▶│                                        │
│  /api/logs/dnsmasq       │     │                                        │
└──────────────────────────┘     │                                        │
                                 │                                        │
┌──────────────────────────┐     │                                        │
│    Pi-hole (API)         │────▶│                                        │
│  /devices                │     │                                        │
│  /top_clients            │     │                                        │
│  /upstreams              │     │                                        │
│  /ftl                    │     │                                        │
│  /system                 │     │                                        │
│  /queries                │     │                                        │
└──────────────────────────┘     │                                        │
                                 │                                        │
┌──────────────────────────┐     │                                        │
│    Public APIs           │────▶│                                        │
│  (multiple)              │     │                                        │
└──────────────────────────┘     │                                        │
                                 │                                        │
┌──────────────────────────┐     │                                        │
│    Localhost Random API  │────▶│                                       │
└──────────────────────────┘     │                                        │
                                 │                                        │
                                 └───────┬──────────────┬──────────────┬──┘
                                         │              │              │
                                 ┌───────▼───┐  ┌───────▼───┐  ┌───────▼───┐
                                 │ Consumer  │  │ Consumer  │  │ Consumer  │
                                 │  Service  │  │  Service  │  │  Service  │
                                 └───────────┘  └───────────┘  └───────────┘
```

---

## 📁 **Project Structure**

```mermaid
graph TD
    subgraph "Project Root"
        A[docker-compose.yml]
        B[.env.example]
        C[requirements.txt]
        D[README.md]
        E[Makefile]
        F[.gitignore]
    end

    subgraph "src/"
        G[producers/]
        H[consumers/]
        I[services/]
        J[models/]
        K[config/]
        L[utils/]
        M[__main__.py]
    end

    subgraph "producers/"
        N[base_producer.py]
        O[pi_hole_file_monitor.py]
        P[pi_hole_api_logs_poller.py]
        Q[pi_hole_data_poller.py]
        R[public_api_fetcher.py]
        S[random_api_fetcher.py]
    end

    subgraph "consumers/"
        T[base_consumer.py]
        U[consumer_1_logs.py]
        V[consumer_2_metrics.py]
        W[consumer_3_external.py]
    end

    subgraph "services/"
        X[api_client.py]
        Y[random_api_server.py]
        Z[kafka_client.py]
    end

    subgraph "models/"
        AA[pi_hole_log.py]
        AB[pi_hole_metric.py]
        AC[public_api_data.py]
        AD[random_data.py]
    end

    subgraph "config/"
        AE[settings.py]
        AF[topics.py]
    end

    subgraph "utils/"
        AG[logger.py]
        AH[file_watcher.py]
        AI[timestamp.py]
    end

    subgraph "tests/"
        AJ[test_producers.py]
        AK[test_consumers.py]
        AL[conftest.py]
    end

    subgraph "scripts/"
        AM[create_topics.sh]
        AN[delete_topics.sh]
        AO[start_producers.sh]
    end

    %% Connections
    A --> G
    B --> G
    C --> G
    D --> G
    E --> G
    F --> G

    G --> H
    G --> I
    G --> J
    G --> K
    G --> L
    G --> M

    H --> N
    H --> O
    H --> P
    H --> Q
    H --> R
    H --> S

    I --> T
    I --> U
    I --> V
    I --> W

    J --> X
    J --> Y
    J --> Z

    K --> AA
    K --> AB
    K --> AC
    K --> AD

    L --> AE
    L --> AF

    M --> AG
    M --> AH
    M --> AI

    N --> AJ
    N --> AK
    N --> AL

    O --> AM
    O --> AN
    O --> AO
```


```
kafka-n-apis/
├── docker-compose.yml                    # Kafka + Zookeeper
├── .env.example                          # Environment template
├── requirements.txt                      # Python dependencies
├── README.md                             # Project documentation
├── .gitignore                            # Files ignored by Git
├── Makefile                              # Useful commands (optional)
│
├── src/                                  # Main source code
│   ├── __init__.py
│   │
│   ├── producers/                        # Producers (send data to Kafka)
│   │   ├── __init__.py
│   │   ├── base_producer.py              # Base class for producers
│   │   ├── pi_hole_file_monitor.py       # Tailing pihole.log → Kafka
│   │   ├── pi_hole_api_logs_poller.py    # Fetching /api/logs/dnsmasq → Kafka
│   │   ├── pi_hole_data_poller.py        # Fetching /devices, /top_clients, /upstreams, etc.
│   │   ├── public_api_fetcher.py         # Public APIs → Kafka
│   │   └── random_api_fetcher.py         # Localhost random API → Kafka
│   │
│   ├── consumers/                        # Consumers (process data from Kafka)
│   │   ├── __init__.py
│   │   ├── base_consumer.py              # Base class for consumers
│   │   ├── consumer_1_logs.py            # Processes DNS logs (file + API)
│   │   ├── consumer_2_metrics.py         # Processes metrics (devices, clients, upstreams, etc.)
│   │   └── consumer_3_external.py        # Processes external data (public APIs + random)
│   │
│   ├── services/                         # Auxiliary services
│   │   ├── __init__.py
│   │   ├── api_client.py                 # HTTP client for external APIs
│   │   ├── random_api_server.py          # Flask server for random data
│   │   └── kafka_client.py               # Kafka client (producer/consumer)
│   │
│   ├── models/                           # Data models (schemas)
│   │   ├── __init__.py
│   │   ├── pi_hole_log.py                # Schema for Pi-hole logs
│   │   ├── pi_hole_metric.py             # Schema for Pi-hole metrics
│   │   ├── public_api_data.py            # Schema for public API data
│   │   └── random_data.py                # Schema for random data
│   │
│   ├── config/                           # Configuration
│   │   ├── __init__.py
│   │   ├── settings.py                   # Loads .env variables
│   │   └── topics.py                     # Kafka topic definitions
│   │
│   ├── utils/                            # Utilities
│   │   ├── __init__.py
│   │   ├── logger.py                     # Logging configuration
│   │   ├── file_watcher.py               # File monitoring (tail -f)
│   │   └── timestamp.py                  # Timestamp manipulation
│   │
│   └── __main__.py                       # Entry point (optional)
│
├── tests/                                # Tests
│   ├── __init__.py
│   ├── test_producers.py                 # Producer tests
│   ├── test_consumers.py                 # Consumer tests
│   └── conftest.py                       # Test configuration
│
├── scripts/                              # Support scripts
│   ├── create_topics.sh                  # Creates Kafka topics
│   ├── delete_topics.sh                  # Removes Kafka topics
│   └── start_producers.sh                # Starts all producers
│
├── data/                                 # Local data (optional)
│   └── logs/                             # Project-generated logs
│
└── mermaid-diagrams/                     # Project Mermaid diagrams
```


---

## 📄 **Main Files Breakdown**

### **Producers (src/producers/)**

| File | Description |
|------|-------------|
| `base_producer.py` | Abstract class with common methods (Kafka connection, message sending, error handling) |
| `pi_hole_file_monitor.py` | Monitors `/var/log/pihole/pihole.log` using `file_watcher.py` and sends lines to topic `pi-hole.logs.file` |
| `pi_hole_api_logs_poller.py` | Polls the `/api/logs/dnsmasq` endpoint every N seconds and sends to `pi-hole.logs.api` |
| `pi_hole_data_poller.py` | Queries endpoints `/devices`, `/top_clients`, `/upstreams`, `/ftl`, `/system`, `/queries` and sends to `pi-hole.data.endpoints` |
| `public_api_fetcher.py` | Makes requests to public APIs (ip-api, viacep, etc.) and sends to `public.api.data` |
| `random_api_fetcher.py` | Queries `http://localhost:5000/random` and sends to `random.data.raw` |

### **Consumers (src/consumers/)**

| File | Description |
|------|-------------|
| `base_consumer.py` | Abstract class with common methods (Kafka connection, message consumption, processing) |
| `consumer_1_logs.py` | Subscribes to topics `pi-hole.logs.file` and `pi-hole.logs.api` and processes DNS logs |
| `consumer_2_metrics.py` | Subscribes to topic `pi-hole.data.endpoints` and processes metrics (devices, top clients, upstreams, FTL, system, queries) |
| `consumer_3_external.py` | Subscribes to topics `public.api.data` and `random.data.raw` and processes external data |

### **Services (src/services/)**

| File | Description |
|------|-------------|
| `api_client.py` | Reusable HTTP client for calling external APIs (error handling, retry, timeouts) |
| `random_api_server.py` | Flask server that generates random data at `/random` |
| `kafka_client.py` | Encapsulates Kafka connection (production and consumption) |

### **Models (src/models/)**

| File | Description |
|------|-------------|
| `pi_hole_log.py` | Schema for Pi-hole logs (timestamp, client, domain, status) |
| `pi_hole_metric.py` | Schema for metrics (devices, top clients, upstreams, etc.) |
| `public_api_data.py` | Schema for public API data (geolocation, etc.) |
| `random_data.py` | Schema for random data (id, value, category, timestamp) |

### **Configuration (src/config/)**

| File | Description |
|------|-------------|
| `settings.py` | Loads `.env` variables using `python-dotenv` |
| `topics.py` | Defines constants with topic names |

### **Utilities (src/utils/)**

| File | Description |
|------|-------------|
| `logger.py` | Configures logging with levels, colors, and format |
| `file_watcher.py` | Monitors files in real-time (tail -f) |
| `timestamp.py` | Functions for timestamp manipulation (formatting, conversion) |

### **Scripts (scripts/)**

| File | Description |
|------|-------------|
| `create_topics.sh` | Creates Kafka topics: `pi-hole.logs.file`, `pi-hole.logs.api`, `pi-hole.data.endpoints`, `public.api.data`, `random.data.raw` |
| `delete_topics.sh` | Removes topics (useful for cleanup) |
| `start_producers.sh` | Starts all producers in the background |

---

---

## Features

- ✅ Pi-hole DNS logs via **local file** (real-time)
- ✅ Pi-hole DNS logs via **API** (remote access)
- ✅ Pi-hole metrics: devices, top clients, upstreams, FTL, system, queries
- ✅ Multiple public APIs fetched as Kafka events
- ✅ Localhost random data generator for testing
- ✅ **Three independent consumers** for parallel processing
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
pip install -r requirements.txt
```

### 4. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your Pi-hole URL, API tokens, and Kafka bootstrap.

### 5. Run the localhost random API (separate terminal)

```bash
python -m producer.random_api_server
```

> Runs a Flask server at `http://localhost:5000/random`

### 6. Run the consumers (three separate terminals)

**Consumer 1 (DNS logs):**
```bash
python -m consumers.consumer_1_logs
```

**Consumer 2 (Metrics and system data):**
```bash
python -m consumers.consumer_2_metrics
```

**Consumer 3 (External and synthetic data):**
```bash
python -m consumers.consumer_3_external
```

## Configuration

| Variable              | Description                     | Default           |
|-----------------------|---------------------------------|-------------------|
| `KAFKA_BOOTSTRAP`     | Kafka bootstrap server          | `localhost:9092`  |
| `PIHOLE_URL`          | Pi-hole admin API URL           | —                 |
| `PIHOLE_API_TOKEN`    | Pi-hole API token               | —                 |
| `PIHOLE_LOG_PATH`     | Path to local pihole.log        | `/var/log/pihole/pihole.log` |
| `RANDOM_API_URL`      | Localhost random API URL        | `http://localhost:5000/random` |

---

## Usage

**Consumer 1 (DNS logs):**
```bash
python -m consumers.consumer_1_logs
```

**Consumer 2 (Metrics and system data):**
```bash
python -m consumers.consumer_2_metrics
```

**Consumer 3 (External and synthetic data):**
```bash
python -m consumers.consumer_3_external
```

---

## Roadmap

- [ ] WebSocket API for real-time dashboards
- [ ] Dead-letter queue for failed events
- [ ] Schema Registry and Avro support
- [ ] Metrics export (Prometheus)
- [ ] Kubernetes manifests
