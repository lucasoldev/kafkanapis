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
    PUBLIC_APIS_TOPIC = os.getenv('PUBLIC_APIS_TOPIC')
    PIHOLE_API_LOG_TOPIC = os.getenv('PIHOLE_API_LOG_TOPIC')

    

    # Pi-hole API Ideas Topics
    PIHOLE_IDEAS_TOPIC_NETWORK_DEVICES = os.getenv('PIHOLE_IDEAS_TOPIC_NETWORK_DEVICES')
    PIHOLE_IDEAS_TOPIC_TOP_CLIENTS = os.getenv('PIHOLE_IDEAS_TOPIC_TOP_CLIENTS')
    PIHOLE_IDEAS_TOPIC_UPSTREAMS = os.getenv('PIHOLE_IDEAS_TOPIC_UPSTREAMS')
    PIHOLE_IDEAS_TOPIC_FTL_STATUS = os.getenv('PIHOLE_IDEAS_TOPIC_FTL_STATUS')
    PIHOLE_IDEAS_TOPIC_QUERIES = os.getenv('PIHOLE_IDEAS_TOPIC_QUERIES')
    
    # PostgreSQL
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    
    # Database names
    PIHOLE_DB_LOCAL_LOGS = os.getenv('PIHOLE_DB_LOCAL_LOGS', 'pihole_local_logs')
    PIHOLE_DB_API_LOGS = os.getenv('PIHOLE_DB_API_LOGS', 'pihole_api_logs')
    DB_PUBLIC_APIS = os.getenv('DB_PUBLIC_APIS', 'public_apis')
    PIHOLE_DB_API_IDEAS = os.getenv('PIHOLE_DB_API_IDEAS', 'pihole_api_ideas')

config = Config()