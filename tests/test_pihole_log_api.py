import requests
import json
import time
from urllib3.exceptions import InsecureRequestWarning
from config import config

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Usa PIHOLE_BASE_URL diretamente
PIHOLE_URL = config.PIHOLE_BASE_URL.rstrip('/')
PASSWORD = config.PIHOLE_ADM_PWD

def get_sid():
    """Authenticate with the Pi-hole API and return the SID."""
    if not PASSWORD:
        print("❌ Password not defined in config (PIHOLE_ADM_PWD).")
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
    
    print("❌ Failed to obtain SID after multiple attempts.")
    return None

def use_sid(endpoint, sid):
    """Use SID to access a protected endpoint."""
    url = f"{PIHOLE_URL}{endpoint}?sid={sid}"
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        return response.json()
    except:
        return None

if __name__ == "__main__":
    sid = get_sid()
    if sid:
        for endpoint in ["/dns/blocking", "/stats/summary", "/stats/top_domains"]:
            data = use_sid(endpoint, sid)
            if data:
                print(f"\n📊 Response from {endpoint}:")
                print(json.dumps(data, indent=2))
