import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Pi-hole
    PIHOLE_BASE_URL = os.getenv("PIHOLE_BASE_URL")
    PIHOLE_DNS_URL = os.getenv("PIHOLE_DNS_URL")
    PIHOLE_ADM_PWD = os.getenv("PIHOLE_ADM_PWD")

    # Kafka
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

    # PostgreSQL
    DATABASE_URL = os.getenv("DATABASE_URL")