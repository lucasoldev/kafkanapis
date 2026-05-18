import json
from confluent_kafka import Consumer
from config import config

# Configurações
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

    print(f"🟢 Consumidor iniciado. Ouvindo tópico: {TOPIC}")
    print("Pressione Ctrl+C para parar.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Erro: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            print(f"📥 Mensagem recebida: {data}")
    except KeyboardInterrupt:
        print("\n🛑 Consumidor interrompido pelo usuário.")
    finally:
        consumer.close()
        print("✅ Consumidor fechado.")

if __name__ == "__main__":
    main()