import re
import requests
import json
import paramiko
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

def test_ftl():
    sid = get_sid()
    if not sid:
        return
    
    url = f"{PIHOLE_URL}/api/info/ftl"
    headers = {"X-FTL-SID": sid}
    
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        data = response.json()
        ftl = data.get("ftl", {})
        print(f"\n⚙️  FTL Status:\n")
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
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_ftl()
