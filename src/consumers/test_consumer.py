import json
from confluent_kafka import Consumer
from config import config

# Settings
KAFKA_BOOTSTRAP = config.KAFKA_BOOTSTRAP
TOPIC = "pi-hole.logs.api.dnsmasq"

def main():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'test-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print(f"🟢 Consumer started. Listening to topic: {TOPIC}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Error: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            print(f"📥 Received message: {data}")
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted by user.")
    finally:
        consumer.close()
        print("✅ Consumer closed.")

if __name__ == "__main__":
    main()
