import sys
import requests
import json
import time
import re
import paramiko
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
from confluent_kafka import Producer
from config import config

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

PIHOLE_URL = config.PIHOLE_DNS_URL.rstrip('/')
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

SHOW_SENT_MESSAGES = True

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        if SHOW_SENT_MESSAGES:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def get_sid():
    if not PASSWORD:
        print("❌ Password not defined in config.")
        return None
    
    url = f"{PIHOLE_URL}/api/auth"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    payload = {"password": PASSWORD}
    
    try:
        response = requests.post(url, headers=headers, json=payload, verify=False)
        response.raise_for_status()
        data = response.json()
        sid = data.get("session", {}).get("sid")
        if sid:
            print(f"✅ SID obtained: {sid}")
            return sid
        else:
            print("❌ Failed to get SID.")
            return None
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return None

def request(endpoint, params=None):
    global SID
    if not SID:
        print("❌ No SID available. Run get_sid() first.")
        return None
    
    url = f"{PIHOLE_URL}/api{endpoint}"
    headers = {"X-FTL-SID": SID}
    full_params = {}
    if params:
        full_params.update(params)
    
    try:
        response = requests.get(url, headers=headers, params=full_params, verify=False)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error accessing {endpoint}: {e}")
        return None

def get_raspberry_uptime():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=config.RASPBERRY_SSH_HOST,
            username=config.RASPBERRY_SSH_USER,
            password=config.RASPBERRY_SSH_PASSWORD,
            timeout=5
        )
        stdin, stdout, stderr = ssh.exec_command('uptime')
        uptime_output = stdout.read().decode('utf-8').strip()
        ssh.close()
        uptime = re.search(r'up\s+(.*?),\s+', uptime_output).group(1).strip()
        return uptime
    except Exception as e:
        return f"Error: {e}"

def produce_api_ideas():
    global SID
    SID = get_sid()
    if not SID:
        return
    
    topics = {
        "network_devices": "pi-hole.ideas.network-devices",
        "top_clients": "pi-hole.ideas.top-clients",
        "upstreams": "pi-hole.ideas.upstreams",
        "ftl_status": "pi-hole.ideas.ftl-status",
        "queries": "pi-hole.ideas.queries"
    }
    
    while True:
        # 1. Network devices
        data = request("/network/devices")
        if data:
            devices = data.get("devices", [])
            if SHOW_SENT_MESSAGES:
                print(f"\n📡 Sending to {topics['network_devices']}:")
                print(f"   Total devices: {len(devices)}")
            if not TEST_MODE and producer:
                producer.produce(
                    topics['network_devices'],
                    key="network_devices",
                    value=json.dumps(data),
                    callback=delivery_report
                )
                producer.flush()
        
        # 2. Top clients
        data = request("/stats/top_clients", {"count": 5})
        if data:
            clients = data.get("clients", [])
            if SHOW_SENT_MESSAGES:
                print(f"\n📡 Sending to {topics['top_clients']}:")
                for i, client in enumerate(clients):
                    print(f"   {i+1}. {client.get('ip', 'N/A')} - {client.get('count', 0)} queries")
            if not TEST_MODE and producer:
                producer.produce(
                    topics['top_clients'],
                    key="top_clients",
                    value=json.dumps(data),
                    callback=delivery_report
                )
                producer.flush()
        
        # 3. Upstream latency
        data = request("/stats/upstreams")
        if data:
            upstreams = data.get("upstreams", [])
            if SHOW_SENT_MESSAGES:
                print(f"\n📡 Sending to {topics['upstreams']}:")
                for upstream in upstreams:
                    name = upstream.get('name', 'N/A')
                    ip = upstream.get('ip', 'N/A')
                    stats = upstream.get('statistics', {})
                    avg_time = stats.get('response', 'N/A')
                    count = upstream.get('count', 0)
                    print(f"   {name} ({ip}): {avg_time:.2f}ms ({count} queries)")
            if not TEST_MODE and producer:
                producer.produce(
                    topics['upstreams'],
                    key="upstreams",
                    value=json.dumps(data),
                    callback=delivery_report
                )
                producer.flush()
        
        # 4. FTL status
        data = request("/info/ftl")
        if data:
            ftl = data.get("ftl", {})
            if SHOW_SENT_MESSAGES:
                print(f"\n📡 Sending to {topics['ftl_status']}:")
                print(f"   PID: {ftl.get('pid', 'N/A')}")
                print(f"   System Uptime: {get_raspberry_uptime()}")
                print(f"   CPU: {ftl.get('%cpu', 0):.2f}%")
                print(f"   Memory: {ftl.get('%mem', 0):.2f}%")
                print(f"   Database: {ftl.get('database', {}).get('gravity', 'N/A')} blocked domains")
            if not TEST_MODE and producer:
                producer.produce(
                    topics['ftl_status'],
                    key="ftl_status",
                    value=json.dumps(data),
                    callback=delivery_report
                )
                producer.flush()
        
        # 5. Queries
        import time as t
        now = t.time()
        yesterday = now - 86400
        data = request("/queries", {
            "from": int(yesterday),
            "until": int(now),
            "limit": 100
        })
        if data:
            queries = data.get("queries", [])
            unique_queries = {}
            for q in queries:
                domain = q.get('domain', 'N/A')
                if domain not in unique_queries:
                    unique_queries[domain] = q
            if SHOW_SENT_MESSAGES:
                print(f"\n📡 Sending to {topics['queries']}:")
                for i, (domain, q) in enumerate(unique_queries.items()):
                    time_unix = q.get('time', 0)
                    dt = datetime.fromtimestamp(time_unix)
                    print(f"   {i+1}. {dt.strftime('%Y-%m-%d %H:%M:%S')} - {domain}")
            if not TEST_MODE and producer:
                producer.produce(
                    topics['queries'],
                    key="queries",
                    value=json.dumps(data),
                    callback=delivery_report
                )
                producer.flush()
        
        time.sleep(10)

def main():
    produce_api_ideas()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Application stopped by user.")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
