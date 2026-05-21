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
# PostgreSQL settings (Faker database)
# ============================================================
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.FAKER_DB  # <-- Variável a ser adicionada no config e .env
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

def ensure_tables(conn):
    """Create tables for each Faker package if they don't exist."""
    cur = conn.cursor()
    
    # Faker tables for each package
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_person (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            name TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            cellphone TEXT,
            cpf TEXT,
            rg TEXT,
            job TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_address (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            street TEXT,
            number TEXT,
            city TEXT,
            state TEXT,
            state_abbr TEXT,
            zipcode TEXT,
            country TEXT,
            full_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_internet (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            url TEXT,
            domain TEXT,
            ipv4 TEXT,
            ipv6 TEXT,
            mac TEXT,
            user_agent TEXT,
            slug TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_text (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            sentence TEXT,
            paragraph TEXT,
            text TEXT,
            word TEXT,
            words TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_numbers (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            random_int INTEGER,
            random_digit INTEGER,
            random_number INTEGER,
            pyint INTEGER,
            pystr TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_dates (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            date TEXT,
            datetime TEXT,
            time TEXT,
            date_this_year TEXT,
            date_this_month TEXT,
            future_date TEXT,
            past_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_credit (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            card_number TEXT,
            card_provider TEXT,
            card_security_code TEXT,
            iban TEXT,
            bic TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_company (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            name TEXT,
            suffix TEXT,
            catch_phrase TEXT,
            industry TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_misc (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            color TEXT,
            hex_color TEXT,
            uuid TEXT,
            file_extension TEXT,
            mime_type TEXT,
            emoji TEXT,
            password TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faker_geo (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            latitude NUMERIC,
            longitude NUMERIC,
            coordinate TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cur.close()

def insert_data(conn, data, table_name, fields):
    """Generic insert function for Faker data."""
    cur = conn.cursor()
    placeholders = ', '.join(['%s'] * len(fields))
    columns = ', '.join(fields)
    values = [data.get(field) for field in fields]
    
    query = f"INSERT INTO {table_name} (timestamp, {columns}) VALUES (%s, {placeholders})"
    cur.execute(query, [datetime.now()] + values)
    conn.commit()
    cur.close()

# ============================================================
# Main consumer loop
# ============================================================
def main():
    conf = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP,
        'group.id': 'faker-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    topics = [
        'fake-data.person',
        'fake-data.address',
        'fake-data.internet',
        'fake-data.text',
        'fake-data.numbers',
        'fake-data.dates',
        'fake-data.credit',
        'fake-data.company',
        'fake-data.misc',
        'fake-data.geo'
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
            
            if SHOW_RECEIVED_MESSAGES:
                package = data.get('package', 'unknown')
                print(f"\n📥 Received from {source}:")
                print(f"   Package: {package}")
                print(f"   Data: {json.dumps(data, indent=2)[:200]}...")
            
            if not TEST_MODE:
                # Map topic to table and fields
                if source == 'fake-data.person':
                    fields = ['name', 'first_name', 'last_name', 'email', 'phone', 'cellphone', 'cpf', 'rg', 'job']
                    insert_data(conn, data, 'faker_person', fields)
                elif source == 'fake-data.address':
                    fields = ['street', 'number', 'city', 'state', 'state_abbr', 'zipcode', 'country', 'full_address']
                    insert_data(conn, data, 'faker_address', fields)
                elif source == 'fake-data.internet':
                    fields = ['url', 'domain', 'ipv4', 'ipv6', 'mac', 'user_agent', 'slug']
                    insert_data(conn, data, 'faker_internet', fields)
                elif source == 'fake-data.text':
                    fields = ['sentence', 'paragraph', 'text', 'word', 'words']
                    insert_data(conn, data, 'faker_text', fields)
                elif source == 'fake-data.numbers':
                    fields = ['random_int', 'random_digit', 'random_number', 'pyint', 'pystr']
                    insert_data(conn, data, 'faker_numbers', fields)
                elif source == 'fake-data.dates':
                    fields = ['date', 'datetime', 'time', 'date_this_year', 'date_this_month', 'future_date', 'past_date']
                    insert_data(conn, data, 'faker_dates', fields)
                elif source == 'fake-data.credit':
                    fields = ['card_number', 'card_provider', 'card_security_code', 'iban', 'bic']
                    insert_data(conn, data, 'faker_credit', fields)
                elif source == 'fake-data.company':
                    fields = ['name', 'suffix', 'catch_phrase', 'industry']
                    insert_data(conn, data, 'faker_company', fields)
                elif source == 'fake-data.misc':
                    fields = ['color', 'hex_color', 'uuid', 'file_extension', 'mime_type', 'emoji', 'password']
                    insert_data(conn, data, 'faker_misc', fields)
                elif source == 'fake-data.geo':
                    fields = ['latitude', 'longitude', 'coordinate']
                    insert_data(conn, data, 'faker_geo', fields)
            
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
