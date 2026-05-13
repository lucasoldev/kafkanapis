import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

class Settings:
    """Configurações do projeto carregadas do .env."""
    
    # Kafka
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
    PIHOLE_LOG_TOPIC = os.getenv('PIHOLE_LOG_TOPIC', 'pi-hole.logs.file')
    
    # Pi-hole via SSH
    RASPBERRY_SSH_HOST = os.getenv('RASPBERRY_SSH_HOST', '192.168.15.160')
    RASPBERRY_SSH_USER = os.getenv('RASPBERRY_SSH_USER', 'pi')
    RASPBERRY_SSH_PASSWORD = os.getenv('RASPBERRY_SSH_PASSWORD', '')
    PIHOLE_LOG_PATH = os.getenv('PIHOLE_LOG_PATH', '/var/log/pihole/pihole.log')
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '5'))
    
    # Chave SSH (opcional)
    SSH_KEY_PATH = os.getenv('SSH_KEY_PATH', '~/.ssh/id_rsa')

settings = Settings()