import json
import psycopg2
from datetime import datetime
from confluent_kafka import Consumer
from config import config

# PostgreSQL settings (from config)
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.POSTGRES_DB
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
        # Parse the log timestamp (assuming current year)
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

def consume_logs():
    """Consume messages from Kafka and insert into PostgreSQL."""
    # Kafka consumer configuration
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP,
        'group.id': 'logs-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([config.PIHOLE_LOG_TOPIC])
    
    conn = connect_db()
    print(f"✅ Connected to PostgreSQL. Consuming topic: {config.PIHOLE_LOG_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)  # 1 second timeout
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            insert_log(conn, data)
            print(f"📥 Inserted: {data.get('domain', 'unknown')}")
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted.")
    finally:
        conn.close()
        consumer.close()

if __name__ == '__main__':
    consume_logs()