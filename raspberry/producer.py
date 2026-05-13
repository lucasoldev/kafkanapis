#!/usr/bin/env python3
import time
import json
import subprocess
from kafka import KafkaProducer
from config import config

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def parse_pihole_log_line(line):
    """Parse a Pi-hole log line and return a structured dictionary."""
    try:
        parts = line.strip().split()
        if len(parts) >= 5:
            timestamp = f"{parts[0]} {parts[1]} {parts[2]}"
            client_ip = parts[3]
            domain = parts[4].replace(':', '')
            return {
                'timestamp': timestamp,
                'client_ip': client_ip,
                'domain': domain,
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
                
                producer.send(config.PIHOLE_LOG_TOPIC, data)
                producer.flush()
                print(f"📨 Sent: {data['domain'] if 'domain' in data else 'raw'}")
    except KeyboardInterrupt:
        print("\n🛑 Monitoring interrupted by user.")
    finally:
        process.terminate()
        producer.close()

if __name__ == '__main__':
    monitor_pihole_log()
