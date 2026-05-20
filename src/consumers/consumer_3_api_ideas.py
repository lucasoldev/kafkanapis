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
PG_DB = config.PIHOLE_DB_API_IDEAS
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
            total_devices INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. Top clients
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ideas_top_clients (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            client_ip TEXT,
            query_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 3. Upstreams
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ideas_upstreams (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            upstream_name TEXT,
            upstream_ip TEXT,
            avg_response_ms NUMERIC,
            query_count INTEGER,
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
    """Insert network devices data (simplified)."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ideas_network_devices (timestamp, total_devices)
        VALUES (%s, %s)
    """, (
        ts,
        data.get("total_devices", 0)
    ))
    conn.commit()
    cur.close()

def insert_top_clients(conn, data, ts):
    """Insert top clients data (one row per client)."""
    cur = conn.cursor()
    clients = data.get("clients", [])
    for client in clients:
        cur.execute("""
            INSERT INTO ideas_top_clients (timestamp, client_ip, query_count)
            VALUES (%s, %s, %s)
        """, (
            ts,
            client.get("ip"),
            client.get("count")
        ))
    conn.commit()
    cur.close()

def insert_upstreams(conn, data, ts):
    """Insert upstreams data (one row per upstream)."""
    cur = conn.cursor()
    upstreams = data.get("upstreams", [])
    for u in upstreams:
        cur.execute("""
            INSERT INTO ideas_upstreams (timestamp, upstream_name, upstream_ip, avg_response_ms, query_count)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            ts,
            u.get("name"),
            u.get("ip"),
            u.get("avg_response", 0),
            u.get("count", 0)
        ))
    conn.commit()
    cur.close()

def insert_ftl_status(conn, data, ts):
    """Insert FTL status data."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ideas_ftl_status (timestamp, pid, system_uptime, cpu_percent, memory_percent, blocked_domains)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        ts,
        data.get("pid"),
        data.get("system_uptime", "N/A"),
        data.get("cpu_percent", 0),
        data.get("memory_percent", 0),
        data.get("blocked_domains", 0)
    ))
    conn.commit()
    cur.close()

def insert_queries(conn, data, ts):
    """Insert queries data (one row per query)."""
    cur = conn.cursor()
    queries = data.get("queries", [])
    for q in queries:
        cur.execute("""
            INSERT INTO ideas_queries (timestamp, domain, query_time)
            VALUES (%s, %s, %s)
        """, (
            ts,
            q.get("domain"),
            q.get("time")  # Já vem formatado como string no produtor
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
