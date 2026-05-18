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

# ============================================================
# DISPLAY OPTION
# ============================================================
SHOW_SENT_MESSAGES = True  # Set to False to disable

def delivery_report(err, msg):
    """Callback to confirm message delivery."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        if SHOW_SENT_MESSAGES:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

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
    
    next_ids = {endpoint: 0 for endpoint, _ in endpoints}
    
    try:
        while True:
            for endpoint, topic in endpoints:
                data = get_logs(endpoint, sid, next_ids[endpoint])
                if data and "log" in data and data["log"]:
                    for entry in data["log"]:
                        if SHOW_SENT_MESSAGES:
                            print(f"\n📤 Sending to {topic}:")
                            print(json.dumps(entry, indent=2))
                        if not TEST_MODE and producer:
                            producer.produce(
                                topic,
                                key=str(entry.get("timestamp")),
                                value=json.dumps(entry),
                                callback=delivery_report
                            )
                            producer.poll(0)
                    next_ids[endpoint] = data.get("nextID", next_ids[endpoint] + 1)
                else:
                    if SHOW_SENT_MESSAGES:
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
