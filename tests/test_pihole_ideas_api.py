import re
import requests
import paramiko
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning
from config import config

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

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

def network_devices():
    data = request("/network/devices")
    if data:
        devices = data.get("devices", [])
        print(f"{len(devices)}")
    else:
        print("❌ No data available.")

def top_clients():
    data = request("/stats/top_clients", {"count": 5})
    if data:
        clients = data.get("clients", [])
        for i, client in enumerate(clients):
            print(f"    {i+1}. {client.get('ip', 'N/A')} - {client.get('count', 0)} queries")
    else:
        print("❌ No data available.")

def upstreams():
    data = request("/stats/upstreams")
    if data:
        upstreams = data.get("upstreams", [])
        for upstream in upstreams:
            name = upstream.get('name', 'N/A')
            ip = upstream.get('ip', 'N/A')
            stats = upstream.get('statistics', {})
            avg_time = stats.get('response', 'N/A')
            count = upstream.get('count', 0)
            print(f"    {name} ({ip}): {avg_time:.2f}ms ({count} queries)")
    else:
        print("❌ No data available.")

def ftl_status():
    data = request("/info/ftl")
    if data:
        ftl = data.get("ftl", {})
        print(f"    PID: {ftl.get('pid', 'N/A')}")
        print(f"    System Uptime: {get_raspberry_uptime()}")
        print(f"    CPU: {ftl.get('%cpu', 0):.2f}%")
        print(f"    Memory: {ftl.get('%mem', 0):.2f}%")
        print(f"    Database: {ftl.get('database', {}).get('gravity', 'N/A')} blocked domains")
        
        # Try to get FTL version from the same endpoint (if available)
        version = ftl.get('version')
        if version:
            print(f"    Version: {version}")
        else:
            # If not available, fetch from /info/version
            try:
                url_version = f"{PIHOLE_URL}/api/info/version"
                response_version = requests.get(url_version, headers=headers, verify=False)
                response_version.raise_for_status()
                data_version = response_version.json()
                version_info = data_version.get("version", {})
                ftl_version = version_info.get("ftl", {}).get("local", {}).get("version", "N/A")
                print(f"    Version: {ftl_version}")
            except Exception:
                print(f"    Version: N/A")
    else:
        print("❌ No data available.")

def queries():
    import time
    now = time.time()
    yesterday = now - 86400  # 24h
    
    data = request("/queries", {
        "from": int(yesterday),
        "until": int(now),
        "limit": 100
    })
    if data:
        queries = data.get("queries", [])
        
        # Remove duplicatas por domínio
        unique_queries = {}
        for q in queries:
            domain = q.get('domain', 'N/A')
            if domain not in unique_queries:
                unique_queries[domain] = q
        

        for i, (domain, q) in enumerate(unique_queries.items()):
            time_unix = q.get('time', 0)
            dt = datetime.fromtimestamp(time_unix)
            print(f"    {i+1}. {dt.strftime('%Y-%m-%d %H:%M:%S')} - {domain}")
    else:
        print("❌ No data available.")

def main():
    print("="*70)
    print("📊 PI-HOLE API IDEAS")
    print("="*70 + "\n")
    
    print("🔐 Authenticating...")

    if get_sid():
        # 1. Network devices
        print("\n🖥️  Network Devices: ", end="")
        network_devices()
        
        # 2. Top clients
        print("\n📊 Top 5 Clients:")
        top_clients()
        
        # 3. Upstream latency
        print("\n⏱️  Upstream Latency:")
        upstreams()
        
        # 4. FTL status
        print("\n⚙️  FTL Status:")
        ftl_status()
        
        # 5. Queries
        print("\n📈 Queries (last 24h, unique domains):")
        queries()

if __name__ == "__main__":
    main()
