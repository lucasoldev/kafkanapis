import sys
import json
import psycopg2
from confluent_kafka import Consumer
from config import config

# Test mode flag
TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no data will be inserted into the database")
    print("=" * 60)

# PostgreSQL settings
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.POSTGRES_DB
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

def insert_log(conn, data, source):
    """Insert a log entry into the database."""
    if TEST_MODE:
        return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dns_logs (timestamp, client_ip, domain, raw, source, timestamp_epoch)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data.get('timestamp'),
        data.get('client_ip'),
        data.get('domain'),
        data.get('raw'),
        data.get('source'),
        data.get('timestamp_epoch')
    ))
    conn.commit()
    cur.close()

def display_message(data, source, is_test=False):
    """Display a message in a clean, beautiful format."""
    timestamp = data.get('timestamp', 'N/A')
    domain = data.get('domain', 'N/A')
    client_ip = data.get('client_ip', 'N/A')
    
    if is_test:
        print("─" * 70)
        print(f"📡 Topic: {source}")
        print(f"🕐 Timestamp: {timestamp}")
        print(f"💻 Client IP: {client_ip}")
        print(f"🌐 Domain: {domain}")
        if 'raw' in data:
            print(f"📄 Raw: {data['raw'][:200]}...")
        print("─" * 70)
    else:
        print(f"📥 Inserted: {timestamp} | {domain} | {source}")

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
            
            if TEST_MODE:
                display_message(data, source, is_test=True)
            else:
                insert_log(conn, data, source)
                display_message(data, source, is_test=False)
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted by user.")
    finally:
        if not TEST_MODE and conn:
            conn.close()
        consumer.close()
        print("✅ Consumer closed.")

if __name__ == "__main__":
    main()
