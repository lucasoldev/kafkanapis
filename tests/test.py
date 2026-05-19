import requests
import json
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
from config import config

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

PIHOLE_URL = config.PIHOLE_DNS_URL.rstrip('/')
PASSWORD = config.PIHOLE_ADM_PWD

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

def test_queries():
    sid = get_sid()
    if not sid:
        return
    
    import time
    now = time.time()
    yesterday = now - 86400  # 24h
    
    url = f"{PIHOLE_URL}/api/queries"
    headers = {"X-FTL-SID": sid}
    params = {
        "from": int(yesterday),
        "until": int(now),
        "limit": 100
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        data = response.json()
        queries = data.get("queries", [])
        
        # Remove duplicatas por domínio
        unique_queries = {}
        for q in queries:
            domain = q.get('domain', 'N/A')
            if domain not in unique_queries:
                unique_queries[domain] = q
        
        print(f"\n📈 Queries (last 24h, unique domains):")
        for i, (domain, q) in enumerate(unique_queries.items()):
            time_unix = q.get('time', 0)
            dt = datetime.fromtimestamp(time_unix)
            print(f"    {i+1}. {dt.strftime('%Y-%m-%d %H:%M:%S')} - {domain}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_queries()