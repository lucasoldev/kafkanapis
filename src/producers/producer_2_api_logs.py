import sys
import requests
import json
import time
from urllib3.exceptions import InsecureRequestWarning
from confluent_kafka import Producer
from config import config

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

PIHOLE_URL = config.PIHOLE_BASE_URL.rstrip('/')
PASSWORD = config.PIHOLE_ADM_PWD
KAFKA_BOOTSTRAP = config.KAFKA_BOOTSTRAP

TEST_MODE = "--test" in sys.argv

if TEST_MODE:
    print("🧪 Running in TEST MODE - no messages will be sent to Kafka")
    print("=" * 60)

if not TEST_MODE:
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
else:
    producer = None

def get_sid():
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
    sid = get_sid()
    if not sid:
        return
    
    endpoints = [
        ("/logs/dnsmasq", "pi-hole.logs.api.dnsmasq"),
        ("/logs/ftl", "pi-hole.logs.api.ftl"),
        ("/logs/webserver", "pi-hole.logs.api.webserver")
    ]
    
    try:
        for endpoint, topic in endpoints:
            print(f"\n📡 Polling {endpoint} -> {topic}")
            next_id = 0
            while True:
                data = get_logs(endpoint, sid, next_id)
                if data and "log" in data and data["log"]:
                    if TEST_MODE:
                        print(f"\n🧪 [TEST] {len(data['log'])} messages would be sent to {topic}")
                        for entry in data["log"][:1]:
                            print(f"    🕐 {entry.get('timestamp')}")
                            print(f"    📝 {entry.get('message')}")
                    else:
                        for entry in data["log"]:
                            producer.produce(topic, key=str(entry.get("timestamp")), value=json.dumps(entry))
                            producer.poll(0)
                        next_id = data.get("nextID", next_id + 1)
                        print(f"📨 Sent {len(data['log'])} messages to {topic}")
                else:
                    print(f"⏳ No new logs for {endpoint}. Waiting...")
                time.sleep(5)
    except KeyboardInterrupt:
        print("\n🛑 Producer interrupted by user.")
    finally:
        if not TEST_MODE and producer:
            producer.flush()
            print("✅ Producer flushed and closed.")

if __name__ == "__main__":
    try:
        produce_logs()
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
