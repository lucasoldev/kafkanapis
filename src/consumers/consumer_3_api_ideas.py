import sys
import json
import psycopg2
import time
from datetime import datetime
from confluent_kafka import Consumer
from config import config

# ============================================================
# TEST MODE
# ============================================================
TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no data will be inserted into the database")
    print("=" * 60)

# ============================================================
# DISPLAY OPTION
# ============================================================
SHOW_RECEIVED_MESSAGES = True  # Set to False to disable

# ============================================================
# PostgreSQL settings (API ideas database)
# ============================================================
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.PIHOLE_DB_API_IDEAS  # <-- Variável do config
PG_USER = config.POSTGRES_USER
PG_PASSWORD = config.POSTGRES_PASSWORD

def connect_db():
    """Connect to PostgreSQL and return the connection."""
    if TEST_MODE:
        return None
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return conn

# ============================================================
# Helper: create table for each topic if not exists
# ============================================================
def ensure_tables(conn):
    """Create tables for each topic if they don't exist."""
    cur = conn.cursor()
    
    # 1. Network devices
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ideas_network_devices (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            devices JSONB,
            total_devices INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. Top clients
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ideas_top_clients (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            clients JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 3. Upstreams
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ideas_upstreams (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            upstreams JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 4. FTL status
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ideas_ftl_status (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            pid INTEGER,
            system_uptime TEXT,
            cpu_percent NUMERIC,
            memory_percent NUMERIC,
            blocked_domains INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 5. Queries
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ideas_queries (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            domain TEXT,
            query_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cur.close()

# ============================================================
# Insert functions for each topic
# ============================================================
def insert_network_devices(conn, data, ts):
    """Insert network devices data."""
    cur = conn.cursor()
    devices = data.get("devices", [])
    cur.execute("""
        INSERT INTO ideas_network_devices (timestamp, devices, total_devices)
        VALUES (%s, %s, %s)
    """, (
        ts,
        json.dumps(devices),
        len(devices)
    ))
    conn.commit()
    cur.close()

def insert_top_clients(conn, data, ts):
    """Insert top clients data."""
    cur = conn.cursor()
    clients = data.get("clients", [])
    cur.execute("""
        INSERT INTO ideas_top_clients (timestamp, clients)
        VALUES (%s, %s)
    """, (
        ts,
        json.dumps(clients)
    ))
    conn.commit()
    cur.close()

def insert_upstreams(conn, data, ts):
    """Insert upstreams data."""
    cur = conn.cursor()
    upstreams = data.get("upstreams", [])
    cur.execute("""
        INSERT INTO ideas_upstreams (timestamp, upstreams)
        VALUES (%s, %s)
    """, (
        ts,
        json.dumps(upstreams)
    ))
    conn.commit()
    cur.close()

def insert_ftl_status(conn, data, ts):
    """Insert FTL status data."""
    cur = conn.cursor()
    ftl = data.get("ftl", {})
    cpu = ftl.get("%cpu", 0)
    mem = ftl.get("%mem", 0)
    db_info = ftl.get("database", {})
    blocked = db_info.get("gravity", 0)
    
    cur.execute("""
        INSERT INTO ideas_ftl_status (timestamp, pid, system_uptime, cpu_percent, memory_percent, blocked_domains)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        ts,
        ftl.get("pid"),
        data.get("system_uptime", "N/A"),
        cpu,
        mem,
        blocked
    ))
    conn.commit()
    cur.close()

def insert_queries(conn, data, ts):
    """Insert queries data (one row per unique domain)."""
    cur = conn.cursor()
    queries = data.get("queries", [])
    unique_queries = {}
    for q in queries:
        domain = q.get("domain", "N/A")
        if domain not in unique_queries:
            unique_queries[domain] = q
    
    for domain, q in unique_queries.items():
        query_time = datetime.fromtimestamp(q.get("time", 0))
        cur.execute("""
            INSERT INTO ideas_queries (timestamp, domain, query_time)
            VALUES (%s, %s, %s)
        """, (
            ts,
            domain,
            query_time
        ))
    conn.commit()
    cur.close()

# ============================================================
# Main consumer loop
# ============================================================
def main():
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP,
        'group.id': 'api-ideas-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    topics = [
        config.PIHOLE_IDEAS_TOPIC_NETWORK_DEVICES,
        config.PIHOLE_IDEAS_TOPIC_TOP_CLIENTS,
        config.PIHOLE_IDEAS_TOPIC_UPSTREAMS,
        config.PIHOLE_IDEAS_TOPIC_FTL_STATUS,
        config.PIHOLE_IDEAS_TOPIC_QUERIES
    ]
    consumer.subscribe(topics)
    
    conn = connect_db()
    if not TEST_MODE:
        ensure_tables(conn)
        print(f"✅ Connected to PostgreSQL. Listening to topics: {topics}")
    else:
        print(f"🧪 TEST MODE: Listening to topics: {topics}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            source = msg.topic()
            ts = datetime.now()
            
            # ========================================================
            # Display received message
            # ========================================================
            if SHOW_RECEIVED_MESSAGES:
                print(f"\n📥 Received from {source}:")
                print(f"   Timestamp: {ts}")
                print(f"   Data: {json.dumps(data, indent=2)[:200]}...")
            
            if not TEST_MODE:
                # Route to appropriate insert function
                if source == config.PIHOLE_IDEAS_TOPIC_NETWORK_DEVICES:
                    insert_network_devices(conn, data, ts)
                elif source == config.PIHOLE_IDEAS_TOPIC_TOP_CLIENTS:
                    insert_top_clients(conn, data, ts)
                elif source == config.PIHOLE_IDEAS_TOPIC_UPSTREAMS:
                    insert_upstreams(conn, data, ts)
                elif source == config.PIHOLE_IDEAS_TOPIC_FTL_STATUS:
                    insert_ftl_status(conn, data, ts)
                elif source == config.PIHOLE_IDEAS_TOPIC_QUERIES:
                    insert_queries(conn, data, ts)
            
            # Small delay to avoid overloading
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted by user.")
    finally:
        if not TEST_MODE and conn:
            conn.close()
        consumer.close()
        print("✅ Consumer closed.")

if __name__ == "__main__":
    main()
