import sys
import json
import random
import time
import requests
import psycopg2
from confluent_kafka import Producer
from config import config

# ============================================================
# Settings
# ============================================================
KAFKA_BOOTSTRAP = config.KAFKA_BOOTSTRAP
KAFKA_TOPIC = config.PUBLIC_APIS_TOPIC

# PostgreSQL config to read the public_apis table
PG_HOST = 'localhost'
PG_PORT = config.POSTGRES_PORT
PG_DB = config.DB_PUBLIC_APIS
PG_USER = config.POSTGRES_USER
PG_PASSWORD = config.POSTGRES_PASSWORD

# ============================================================
# Test mode
# ============================================================
TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no messages will be sent to Kafka")
    print("=" * 60)

if not TEST_MODE:
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
else:
    producer = None

SHOW_SENT_MESSAGES = True

# ============================================================
# PostgreSQL connector
# ============================================================
def fetch_apis_from_db():
    """Fetch all APIs from the public_apis table."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SELECT id, site_name, url, get_endpoint FROM public_apis ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# ============================================================
# Request function
# ============================================================
def call_api(site_name, url, endpoint):
    """Make a request (GET or POST) and return the result."""
    try:
        # Special case for 4Devs (POST for CPF generation)
        if '4devs' in url.lower() and 'ferramentas_online.php' in endpoint:
            response = requests.post(
                endpoint,
                data="acao=gerar_cpf&pontuacao=N&cpf_estado=",
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
        else:
            # Standard GET request
            response = requests.get(endpoint, timeout=10)
        
        response.raise_for_status()
        return response.json() if response.text else response.text
    except Exception as e:
        return f"Error: {str(e)}"

# ============================================================
# Kafka callback
# ============================================================
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        if SHOW_SENT_MESSAGES and not TEST_MODE:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

# ============================================================
# Main producer loop
# ============================================================
def produce_public_apis_loop():
    """Infinite loop that randomly iterates through all APIs."""
    print(f"📡 Starting Public APIs Producer. Target topic: {KAFKA_TOPIC}")
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP}\n")
    
    while True:
        # Fetch all APIs from the database
        apis = fetch_apis_from_db()
        if not apis:
            print("❌ No APIs found in the database.")
            time.sleep(10)
            continue
        
        # Shuffle the list
        random.shuffle(apis)
        if SHOW_SENT_MESSAGES:
            print(f"🔄 Starting new cycle with {len(apis)} APIs (shuffled)\n")
        
        for idx, (api_id, site_name, url, endpoint) in enumerate(apis, 1):
            if SHOW_SENT_MESSAGES:
                print(f"📡 [{idx}/{len(apis)}] Calling {site_name}...")
            
            # Make the request
            result = call_api(site_name, url, endpoint)
            
            # Build the Kafka payload
            payload = {
                'api_id': api_id,
                'site_name': site_name,
                'url': url,
                'endpoint': endpoint,
                'result': result,
                'timestamp': time.time()
            }
            
            if SHOW_SENT_MESSAGES:
                print(f"   Result: {str(result)[:100]}...")
            
            if not TEST_MODE and producer:
                producer.produce(
                    KAFKA_TOPIC,
                    value=json.dumps(payload),
                    callback=delivery_report
                )
                producer.poll(0)
            
            # Small pause between requests to avoid overloading
            time.sleep(2)
        
        if SHOW_SENT_MESSAGES:
            print("✅ Cycle completed. Restarting with new shuffle...\n")
        
        # Small pause between cycles
        time.sleep(5)

def main():
    try:
        produce_public_apis_loop()
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()

if __name__ == '__main__':
    main()