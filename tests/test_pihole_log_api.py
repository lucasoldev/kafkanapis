import requests
import json
import time
from urllib3.exceptions import InsecureRequestWarning
from config import config

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

PIHOLE_URL = config.PIHOLE_BASE_URL.rstrip('/')
PASSWORD = config.PIHOLE_ADM_PWD

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

if __name__ == "__main__":
    sid = get_sid()
    if not sid:
        exit(1)
    
    endpoints = ["/logs/dnsmasq", "/logs/ftl", "/logs/webserver"]
    
    for endpoint in endpoints:
        print(f"\n📡 Fetching logs from {endpoint}")
        data = get_logs(endpoint, sid, nextID=0)
        if data and "log" in data:
            print(f"✅ Got {len(data['log'])} lines")
            print(f"📄 nextID: {data.get('nextID')}")
            print(f"📂 File: {data.get('file')}")
            if data['log']:
                for entry in data['log'][:1]:  # Apenas o primeiro registro
                    print(f"  🕐 {entry.get('timestamp')}")
                    print(f"  📝 {entry.get('message')}")
        else:
            print("❌ No logs or error.")
