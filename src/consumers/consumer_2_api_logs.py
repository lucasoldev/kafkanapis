from datetime import datetime
import sys
import json
import psycopg2
import time
from confluent_kafka import Consumer
from config import config

# Test mode flag
TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no data will be inserted into the database")
    print("=" * 60)

# ============================================================
# DISPLAY OPTION
# ============================================================
SHOW_RECEIVED_MESSAGES = True  # Set to False to disable

# PostgreSQL settings (API logs database)
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.PIHOLE_DB_API_LOGS  # <-- Usa a variável do config
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

from datetime import datetime

def insert_log(conn, data, endpoint):
    if TEST_MODE:
        return
    cur = conn.cursor()
    
    # Converts the Unix timestamp to TIMESTAMP
    timestamp = datetime.fromtimestamp(data.get('timestamp')).strftime('%Y-%m-%d %H:%M:%S')
    
    cur.execute("""
        INSERT INTO api_logs (timestamp, message, prio, origin)
        VALUES (%s, %s, %s, %s)
    """, (
        timestamp,
        data.get('message'),
        data.get('prio'),
        endpoint
    ))
    conn.commit()
    cur.close()

def main():
    """Main consumer loop."""
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP,
        'group.id': 'api-logs-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    topics = [
        "pi-hole.logs.api.dnsmasq",
        "pi-hole.logs.api.ftl",
        "pi-hole.logs.api.webserver"
    ]
    consumer.subscribe(topics)
    
    conn = connect_db()
    if not TEST_MODE:
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
            endpoint = source.split('.')[-1]  # 'dnsmasq', 'ftl', 'webserver'
            
            # ========================================================
            # Optional display of received message
            # Comment the line below to disable
            # ========================================================
            if SHOW_RECEIVED_MESSAGES:
                print(f"\n📥 Received from {source}:")
                print(f"   Timestamp: {data.get('timestamp', 'N/A')}")
                print(f"   Message: {data.get('message', 'N/A')}")
                print(f"   Priority: {data.get('prio', 'N/A')}")
                print(f"   Origin: {endpoint}")
            
            if not TEST_MODE:
                insert_log(conn, data, endpoint)
            
            # ========================================================
            # Sleep to control processing speed
            # Adjust the value as needed
            # ========================================================
            time.sleep(0.1)  # Small delay to avoid overloading
            
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted by user.")
    finally:
        if not TEST_MODE and conn:
            conn.close()
        consumer.close()
        print("✅ Consumer closed.")

if __name__ == "__main__":
    main()
