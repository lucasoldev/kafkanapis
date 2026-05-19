import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Project configuration loaded from .env."""
    
    # Network
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '30'))
    
    # Raspberry
    RASPBERRY_SSH_HOST = os.getenv('RASPBERRY_SSH_HOST')
    RASPBERRY_SSH_USER = os.getenv('RASPBERRY_SSH_USER')
    RASPBERRY_SSH_PASSWORD = os.getenv('RASPBERRY_SSH_PASSWORD')
    
    # Pi-hole
    PIHOLE_BASE_URL = os.getenv('PIHOLE_BASE_URL')
    PIHOLE_DNS_URL = os.getenv('PIHOLE_DNS_URL')
    PIHOLE_ADM_PWD = os.getenv('PIHOLE_ADM_PWD')
    PIHOLE_LOG_PATH = os.getenv('PIHOLE_LOG_PATH')
    
    # Kafka
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')
    PIHOLE_LOCAL_LOG_TOPIC = os.getenv('PIHOLE_LOCAL_LOG_TOPIC')
    PIHOLE_API_LOG_TOPIC = os.getenv('PIHOLE_API_LOG_TOPIC')
    
    # PostgreSQL
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    
    # Database names
    PIHOLE_DB_LOCAL_LOGS = os.getenv('PIHOLE_DB_LOCAL_LOGS', 'pihole_local_logs')
    PIHOLE_DB_API_LOGS = os.getenv('PIHOLE_DB_API_LOGS', 'pihole_api_logs')

config = Config()