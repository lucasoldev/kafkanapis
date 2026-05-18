#!/usr/bin/env python3
import sys
import time
import json
import subprocess
from kafka import KafkaProducer
from config import config

# ============================================================
# TEST MODE
# ============================================================
TEST_MODE = "--test" in sys.argv
if TEST_MODE:
    print("🧪 Running in TEST MODE - no messages will be sent to Kafka")
    print("=" * 60)

# ============================================================
# DISPLAY OPTION
# ============================================================
SHOW_SENT_MESSAGES = True  # Set to False to disable

# Connect to Kafka (only if not in test mode)
if not TEST_MODE:
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
else:
    producer = None

def parse_pihole_log_line(line):
    """Parse a Pi-hole log line and return a structured dictionary."""
    try:
        parts = line.strip().split()
        if len(parts) >= 5:
            timestamp = f"{parts[0]} {parts[1]} {parts[2]}"
            
            # Try to find the client IP (if there is "from <IP>")
            client_ip = None
            for i, part in enumerate(parts):
                if part == 'from' and i + 1 < len(parts):
                    client_ip = parts[i + 1]
                    break
            
            # Extract the domain (usually after "cached" or "query")
            domain = None
            for i, part in enumerate(parts):
                if part == 'cached' and i + 1 < len(parts):
                    domain = parts[i + 1]
                    break
                if part == 'query' and i + 2 < len(parts):
                    domain = parts[i + 2]
                    break
                if part == 'gravity' and i + 2 < len(parts):
                    domain = parts[i + 2]
                    break
            
            return {
                'timestamp': timestamp,
                'client_ip': client_ip if client_ip else 'unknown',
                'domain': domain if domain else 'unknown',
                'raw': line.strip()
            }
        return {'raw': line.strip()}
    except Exception as e:
        return {'raw': line.strip(), 'error': str(e)}

def monitor_pihole_log():
    """Monitor the Pi-hole log file in real-time using tail -f."""
    print(f"🔍 Monitoring {config.PIHOLE_LOG_PATH}...")
    print(f"📤 Sending to topic: {config.PIHOLE_LOG_TOPIC}")
    print(f"📡 Kafka: {config.KAFKA_BOOTSTRAP}")
    
    process = subprocess.Popen(
        ['tail', '-f', config.PIHOLE_LOG_PATH],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    try:
        for line in iter(process.stdout.readline, ''):
            if line:
                data = parse_pihole_log_line(line)
                data['source'] = 'pi-hole-file'
                data['timestamp_epoch'] = time.time()
                
                # In test mode, always display the data
                if TEST_MODE:
                    print(f"\n🧪 [TEST] Message would be sent to {config.PIHOLE_LOG_TOPIC}:")
                    print(json.dumps(data, indent=2))
                elif SHOW_SENT_MESSAGES:
                    print(f"\n📤 Sending to {config.PIHOLE_LOG_TOPIC}:")
                    print(json.dumps(data, indent=2))
                
                if not TEST_MODE and producer:
                    producer.send(config.PIHOLE_LOG_TOPIC, data)
                    producer.flush()
                    if SHOW_SENT_MESSAGES:
                        print(f"✅ Message sent to {config.PIHOLE_LOG_TOPIC}")
    except KeyboardInterrupt:
        print("\n🛑 Monitoring interrupted by user.")
    finally:
        if not TEST_MODE and producer:
            producer.close()
            print("✅ Producer closed.")

if __name__ == '__main__':
    monitor_pihole_log()
