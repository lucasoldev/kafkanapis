import json
import psycopg2
import sys
import re
from datetime import datetime
from confluent_kafka import Consumer
from config import config

# --- Test mode ---
TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no data will be inserted into the database")
    print("=" * 60)

# PostgreSQL settings (from config)
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.PIHOLE_DB_LOCAL_LOGS  # <-- New local db
PG_USER = config.POSTGRES_USER
PG_PASSWORD = config.POSTGRES_PASSWORD

def connect_db():
    """Connect to PostgreSQL and return the connection."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return conn

def insert_log(conn, data):
    """Insert a log line into the database."""
    cur = conn.cursor()
    
    # Convert timestamp from "May 14 00:40:02" to "2026-05-14 00:40:02"
    raw_timestamp = data.get('timestamp')
    try:
        dt = datetime.strptime(raw_timestamp, '%b %d %H:%M:%S')
        dt = dt.replace(year=datetime.now().year)
        timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        timestamp = None
    
    cur.execute("""
        INSERT INTO dns_logs (timestamp, client_ip, domain, raw, source, timestamp_epoch)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        timestamp,
        data.get('client_ip'),
        data.get('domain'),
        data.get('raw'),
        data.get('source'),
        data.get('timestamp_epoch')
    ))
    conn.commit()
    cur.close()

def parse_log_line(line):
    """Parse a Pi-hole log line and extract timestamp, client_ip, and domain."""
    parts = line.strip().split()
    timestamp = f"{parts[0]} {parts[1]} {parts[2]}"
    
    # Default values
    client_ip = "unknown"
    domain = "unknown"
    
    # Pattern 1: "cached" or "cached-stale" - e.g. "cached-stale api.docker.com is 2600:..."
    if "cached" in line:
        if len(parts) > 4:
            domain = parts[4]
            if len(parts) > 6:
                ip = parts[6]
                if re.match(r'^(\d{1,3}\.){3}\d{1,3}$', ip) or ':' in ip:
                    client_ip = ip
        return {
            'timestamp': timestamp,
            'client_ip': client_ip,
            'domain': domain,
            'raw': line.strip()
        }
    
    # Pattern 2: "from" - e.g. "query[A] google.com from 192.168.1.50"
    if "from" in line:
        from_index = parts.index('from')
        if from_index > 0 and from_index + 1 < len(parts):
            client_ip = parts[from_index + 1]
            if from_index > 2:
                domain = parts[from_index - 1]
        return {
            'timestamp': timestamp,
            'client_ip': client_ip,
            'domain': domain,
            'raw': line.strip()
        }
    
    # Pattern 3: "gravity" - e.g. "gravity blocked sessions.bugsnag.com is 0.0.0.0"
    if "gravity" in line:
        if len(parts) > 3:
            domain = parts[3]
        return {
            'timestamp': timestamp,
            'client_ip': "gravity",
            'domain': domain,
            'raw': line.strip()
        }
    
    # Fallback: return raw
    return {
        'timestamp': timestamp,
        'client_ip': "unknown",
        'domain': "unknown",
        'raw': line.strip()
    }

def parse_and_display(data):
    """Parse and display a log line for debugging (TEST MODE)."""
    raw = data.get('raw', 'N/A')
    parsed = parse_log_line(raw)
    
    print("=" * 60)
    print(f"📄 RAW: {raw}")
    print(f"🕐 Timestamp: {parsed['timestamp']}")
    print(f"💻 Client IP: {parsed['client_ip']}")
    print(f"🌐 Domain: {parsed['domain']}")
    print(f"📦 Source: {data.get('source', 'N/A')}")
    print(f"⏱️ Epoch: {data.get('timestamp_epoch', 'N/A')}")
    print("=" * 60)
    print()  # Simple line break between records

def display_log(data):
    """Display a log line in production mode (cleaner output)."""
    raw = data.get('raw', 'N/A')
    parsed = parse_log_line(raw)
    
    print(f"🕐 {parsed['timestamp']} | 💻 {parsed['client_ip']} | 🌐 {parsed['domain']}")
    print()  # Single line break between records

def consume_logs():
    """Consume messages from Kafka and insert into PostgreSQL."""
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP,
        'group.id': 'logs-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([config.PIHOLE_LOCAL_LOG_TOPIC])  # <-- Novo tópico local
    
    if not TEST_MODE:
        conn = connect_db()
        print(f"✅ Connected to PostgreSQL. Consuming topic: {config.PIHOLE_LOCAL_LOG_TOPIC}")
    else:
        print(f"🧪 TEST MODE: Consuming topic: {config.PIHOLE_LOCAL_LOG_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            if TEST_MODE:
                parse_and_display(data)
            else:
                # Parse the raw line before inserting
                parsed = parse_log_line(data.get('raw', ''))
                # Update data with parsed values
                data['client_ip'] = parsed['client_ip']
                data['domain'] = parsed['domain']
                insert_log(conn, data)
                display_log(data)
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted.")
    finally:
        if not TEST_MODE:
            conn.close()
        consumer.close()

if __name__ == '__main__':
    try:
        consume_logs()
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
