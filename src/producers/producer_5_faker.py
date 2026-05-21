import sys
import json
import time
import random
from faker import Faker
from confluent_kafka import Producer
from config import config

# Configurações
KAFKA_BOOTSTRAP = config.KAFKA_BOOTSTRAP

TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no messages will be sent to Kafka")
    print("=" * 60)

if not TEST_MODE:
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
else:
    producer = None

SHOW_SENT_MESSAGES = True

# Inicializa o Faker para português do Brasil
fake = Faker('pt_BR')

# Carrega os pacotes do arquivo JSON
with open('faker_packages.json', 'r', encoding='utf-8') as f:
    PACKAGES = json.load(f)

def generate_package(package_name):
    """Gera um pacote completo de dados chamando todos os métodos da categoria."""
    package = PACKAGES[package_name]
    data = {}
    for method in package['methods']:
        try:
            generator = getattr(fake, method)
            data[method] = generator()
        except:
            data[method] = None
    return data

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        if SHOW_SENT_MESSAGES:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_fake_data():
    """Loop infinito escolhendo aleatoriamente um pacote do Faker e enviando para o tópico correspondente."""
    print(f"📡 Generating random fake data for {len(PACKAGES)} topics")
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
    
    try:
        while True:
            # Escolhe um pacote aleatório
            package_name = random.choice(list(PACKAGES.keys()))
            package = PACKAGES[package_name]
            topic = package['topic']
            
            # Gera os dados completos do pacote
            data = generate_package(package_name)
            data['package'] = package_name
            data['timestamp'] = time.time()
            
            if SHOW_SENT_MESSAGES:
                print(f"\n📡 Sending to {topic}:")
                print(f"   Package: {package_name}")
                print(f"   Data: {json.dumps(data, indent=2)[:200]}...")
            
            if not TEST_MODE and producer:
                producer.produce(topic, value=json.dumps(data), key=package_name, callback=delivery_report)
                producer.flush()
            elif TEST_MODE:
                print("🧪 TEST MODE: Data generated but not sent to Kafka")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 Producer interrupted.")
    finally:
        if producer:
            producer.close()

if __name__ == '__main__':
    produce_fake_data()
