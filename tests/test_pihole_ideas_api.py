import requests
import json
from urllib3.exceptions import InsecureRequestWarning
from config import config

# Disable SSL warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# URL base (sem /api/ no final)
PIHOLE_URL = config.PIHOLE_DNS_URL.rstrip('/')
SID = None
PASSWORD = config.PIHOLE_ADM_PWD

def get_sid():
    global SID
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
        SID = data.get("session", {}).get("sid")
        if SID:
            print(f"✅ SID obtained: {SID}")
            return SID
        else:
            print("❌ Failed to get SID.")
            return None
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return None

def request(endpoint, params=None):
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

def network_devices():
    data = request("/network/devices")
    if data:
        devices = data.get("devices", [])
        print(f"\n    Total Devices: {len(devices)}")
    else:
        print("❌ No data available.")

def top_clients():
    data = request("/stats/top_clients", {"count": 5})
    if data:
        clients = data.get("clients", [])
        print(f"\n    Top Clients (top 5):")
        for i, client in enumerate(clients):
            print(f"    {i+1}. {client.get('ip', 'N/A')} - {client.get('count', 0)} queries")
    else:
        print("❌ No data available.")

def upstreams():
    data = request("/stats/upstreams")
    if data:
        upstreams = data.get("upstreams", [])
        print(f"\n⏱️  Upstream Latency (ms):")
        for upstream in upstreams:
            name = upstream.get('name', 'N/A')
            ip = upstream.get('ip', 'N/A')
            stats = upstream.get('statistics', {})
            avg_time = stats.get('response', 'N/A')
            count = upstream.get('count', 0)
            print(f"    {name} ({ip}): {avg_time:.2f}ms ({count} queries)")
    else:
        print("❌ No data available.")

def main():
    print("="*70)
    print("📊 PI-HOLE API IDEAS")
    print("="*70 + "\n")
    
    print("🔐 Authenticating...")

    if get_sid():
        # 1. Network devices
        print("\n🖥️  Network Devices:")
        network_devices()
        
        # 2. Top clients
        print("\n📊 Top Clients:")
        top_clients()
        
        # 3. Upstream latency
        print("\n⏱️  Upstream Latency:")
        upstreams()

if __name__ == "__main__":
    main()