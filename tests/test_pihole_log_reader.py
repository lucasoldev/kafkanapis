import time
import paramiko
from config import config

def get_pihole_log_lines():
    """Get the last lines of the Pi-hole log via SSH with sudo."""
    try:
        # Connect via SSH
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=config.RASPBERRY_SSH_HOST,
            username=config.RASPBERRY_SSH_USER,
            password=config.RASPBERRY_SSH_PASSWORD,
            timeout=10
        )
        
        # Execute command to read the last 100 lines of the log (with sudo)
        stdin, stdout, stderr = ssh.exec_command(f'sudo tail -100 {config.PIHOLE_LOG_PATH}')
        
        # Check for errors in the command (stderr)
        error_output = stderr.read().decode('utf-8').strip()
        if error_output:
            if "Permission denied" in error_output:
                print("❌ Permission error: User does not have permission to execute 'sudo tail'.")
                print("   Configure on Raspberry Pi: 'sudo visudo' and add:")
                print(f"   {config.RASPBERRY_SSH_USER} ALL=(ALL) NOPASSWD: /usr/bin/tail")
                return []
            elif "No such file or directory" in error_output:
                print(f"❌ File not found: {config.PIHOLE_LOG_PATH}")
                print("   Check if the file exists on the Raspberry Pi.")
                return []
            else:
                print(f"❌ SSH command error: {error_output}")
                return []
        
        lines = stdout.read().decode('utf-8').splitlines()
        ssh.close()
        return lines
        
    except paramiko.AuthenticationException:
        print("❌ Authentication error: Incorrect username or password.")
        print("   Check the credentials in the config file.")
        return []
    except paramiko.SSHException as e:
        print(f"❌ SSH connection error: {e}")
        print("   Check if the Raspberry Pi is powered on and reachable.")
        return []
    except TimeoutError:
        print("❌ Timeout: The Raspberry Pi did not respond within 10 seconds.")
        print("   Check the network connection and the Raspberry Pi's IP address.")
        return []
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []

def parse_pihole_log_line(line):
    """Parse a Pi-hole log line and return a structured dictionary."""
    try:
        # Example: "May 12 11:46:02 dnsmasq[252714]: cached youtube-ui.l.google.com is 172.217.29.206"
        parts = line.strip().split()
        
        # Expected format: [Month Day Hour:Minute:Second] [process]: [message]
        if len(parts) >= 5:
            # Extract timestamp (May 12 11:46:02)
            timestamp = f"{parts[0]} {parts[1]} {parts[2]}"
            
            # Extract client IP (if present) and domain
            # Parts after process: "cached", "youtube-ui.l.google.com", "is", "172.217.29.206"
            rest = parts[3:]
            
            # The domain is usually the second item after the timestamp
            if len(rest) >= 2:
                # Remove ":" from the end of the process
                process = rest[0].replace(':', '')
                domain_or_ip = rest[1] if len(rest) > 1 else ''
                
                return {
                    'timestamp': timestamp,
                    'process': process,
                    'domain': domain_or_ip.replace(':', ''),
                    'raw': line.strip(),
                    'parsed': True
                }
            else:
                return {
                    'timestamp': timestamp,
                    'raw': line.strip(),
                    'parsed': False
                }
        else:
            return {'raw': line.strip(), 'parsed': False}
    except Exception as e:
        return {'raw': line.strip(), 'error': str(e), 'parsed': False}

def test_log_reader():
    """Test the Pi-hole log reader and print to the screen."""
    print(f"🔍 Connecting to {config.RASPBERRY_SSH_HOST} via SSH...")
    print(f"📂 Reading {config.PIHOLE_LOG_PATH}\n")
    
    lines = get_pihole_log_lines()
    
    if not lines:
        print("\n✅ Diagnosis: No lines were read. Check the messages above.")
        return
    
    print(f"📊 Total lines read: {len(lines)}\n")
    print("=" * 60)
    print("📋 Last 20 lines of the Pi-hole log:")
    print("=" * 60)
    
    # Show the last 20 lines
    for i, line in enumerate(lines[-20:], 1):
        parsed = parse_pihole_log_line(line)
        print(f"\n[{i}] Raw: {line}")
        print(f"    Parsed: {parsed}")
    
    print("\n" + "=" * 60)
    print("✅ Test completed!")

if __name__ == '__main__':
    test_log_reader()
