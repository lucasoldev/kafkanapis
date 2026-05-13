import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Project configuration loaded from .env."""
    
    # Kafka
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
    PIHOLE_LOG_TOPIC = os.getenv('PIHOLE_LOG_TOPIC', 'pi-hole.logs.file')
    
    # Pi-hole
    PIHOLE_LOG_PATH = os.getenv('PIHOLE_LOG_PATH', '/var/log/pihole/pihole.log')
    
    # Optional
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))

config = Config()

