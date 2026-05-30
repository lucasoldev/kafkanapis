import sys
import json
import psycopg2
import time
from datetime import datetime
from confluent_kafka import Consumer
from config import config

# ============================================================
# Test mode
# ============================================================
TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no data will be inserted into the database")
    print("=" * 60)

# ============================================================
# Display option
# ============================================================
SHOW_RECEIVED_MESSAGES = True

# ============================================================
# PostgreSQL settings
# ============================================================
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.DB_PUBLIC_APIS
PG_USER = config.POSTGRES_USER
PG_PASSWORD = config.POSTGRES_PASSWORD

# ============================================================
# Database connection
# ============================================================
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
# Ensure table exists
# ============================================================
def ensure_tables(conn):
    """Create table for public API results if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public_apis_results (
            id SERIAL PRIMARY KEY,
            api_id INTEGER,
            site_name TEXT,
            url TEXT,
            endpoint TEXT,
            result TEXT,
            result_size INTEGER,
            result_type TEXT,
            timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()

# ============================================================
# Insert function
# ============================================================
def insert_result(conn, data):
    """Insert a single API result into the database."""
    cur = conn.cursor()
    
    # Get data from the message
    api_id = data.get('api_id')
    site_name = data.get('site_name')
    url = data.get('url')
    endpoint = data.get('endpoint')
    result = data.get('result')
    timestamp = datetime.fromtimestamp(data.get('timestamp', time.time()))
    
    # Determine result type and size
    result_str = json.dumps(result) if isinstance(result, (dict, list)) else str(result)
    result_size = len(result_str)
    result_type = type(result).__name__
    
    cur.execute("""
        INSERT INTO public_apis_results (api_id, site_name, url, endpoint, result, result_size, result_type, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        api_id,
        site_name,
        url,
        endpoint,
        result_str,
        result_size,
        result_type,
        timestamp
    ))
    conn.commit()
    cur.close()

# ============================================================
# Main consumer loop
# ============================================================
def main():
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP,
        'group.id': 'public-apis-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([config.PUBLIC_APIS_TOPIC])
    
    conn = connect_db()
    if not TEST_MODE:
        ensure_tables(conn)
        print(f"✅ Connected to PostgreSQL. Listening to topic: {config.PUBLIC_APIS_TOPIC}")
    else:
        print(f"🧪 TEST MODE: Listening to topic: {config.PUBLIC_APIS_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            if SHOW_RECEIVED_MESSAGES:
                print(f"\n📥 Received from {config.PUBLIC_APIS_TOPIC}:")
                print(f"   API: {data.get('site_name')}")
                print(f"   Endpoint: {data.get('endpoint')}")
                print(f"   Result type: {type(data.get('result')).__name__}")
                print(f"   Result size: {len(str(data.get('result')))} characters")
                print(f"   Timestamp: {data.get('timestamp')}")
            
            if not TEST_MODE:
                insert_result(conn, data)
                print(f"✅ Inserted: {data.get('site_name')}")
            
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted by user.")
    finally:
        if not TEST_MODE and conn:
            conn.close()
        consumer.close()
        print("✅ Consumer closed.")

if __name__ == '__main__':
    main()