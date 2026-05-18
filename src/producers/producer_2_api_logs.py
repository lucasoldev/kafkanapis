import requests
import json
import time
from urllib3.exceptions import InsecureRequestWarning
from confluent_kafka import Producer
from config import config

# Desabilita avisos de SSL (para certificado autoassinado)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Configurações
PIHOLE_URL = config.PIHOLE_BASE_URL.rstrip('/')
PASSWORD = config.PIHOLE_ADM_PWD
KAFKA_BOOTSTRAP = config.KAFKA_BOOTSTRAP

# Conecta ao Kafka
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    """Callback para confirmar entrega da mensagem."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def get_sid():
    """Authenticate with the Pi-hole API and return the SID."""
    if not PASSWORD:
        print("❌ Password not defined in config.")
        return None
    
    url = f"{PIHOLE_URL}/auth"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    payload = {"password": PASSWORD}
    
    for attempt in range(3):
        try:
            response = requests.post(url, headers=headers, json=payload, verify=False)
            response.raise_for_status()
            data = response.json()
            sid = data.get("session", {}).get("sid")
            if sid:
                print(f"✅ SID obtained: {sid}")
                return sid
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Attempt {attempt+1}: Error - {e}")
        time.sleep(2)
    
    print("❌ Failed to obtain SID.")
    return None

def get_logs(endpoint, sid, nextID=None):
    """Fetch logs from a Pi-hole endpoint with optional nextID."""
    url = f"{PIHOLE_URL}{endpoint}"
    params = {"sid": sid}
    if nextID is not None:
        params["nextID"] = nextID
    
    try:
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error accessing {endpoint}: {e}")
        return None

def produce_logs():
    """Main loop: fetch logs from API and send to Kafka."""
    sid = get_sid()
    if not sid:
        return
    
    # Endpoints de log e seus tópicos correspondentes
    endpoints = [
        ("/logs/dnsmasq", "pi-hole.logs.api.dnsmasq"),
        ("/logs/ftl", "pi-hole.logs.api.ftl"),
        ("/logs/webserver", "pi-hole.logs.api.webserver")
    ]
    
    # Para cada endpoint, faz o polling
    try:
        for endpoint, topic in endpoints:
            print(f"\n📡 Polling {endpoint} -> {topic}")
            next_id = 0
            while True:
                data = get_logs(endpoint, sid, next_id)
                if data and "log" in data and data["log"]:
                    for entry in data["log"]:
                        producer.produce(
                            topic,
                            key=str(entry.get("timestamp")),
                            value=json.dumps(entry),
                            callback=delivery_report
                        )
                        producer.poll(0)
                    next_id = data.get("nextID", next_id + 1)
                    print(f"📨 Sent {len(data['log'])} messages to {topic}")
                else:
                    print(f"⏳ No new logs for {endpoint}. Waiting...")
                time.sleep(5)
    except KeyboardInterrupt:
        print("\n🛑 Producer interrupted by user.")
    finally:
        producer.flush()
        print("✅ Producer flushed and closed.")

if __name__ == "__main__":
    try:
        produce_logs()
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")