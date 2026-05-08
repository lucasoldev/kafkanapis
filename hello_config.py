from config import Config

print("=== Pi-hole ===")
print(f"Base URL (IP):  {Config.PIHOLE_BASE_URL}")
print(f"Base URL (DNS): {Config.PIHOLE_DNS_URL}")
print(f"ADM Password:      {Config.PIHOLE_ADM_PWD}")

print("\n=== Kafka ===")
print(f"Bootstrap:      {Config.KAFKA_BOOTSTRAP}")

print("\n=== PostgreSQL ===")
print(f"Database URL:   {Config.DATABASE_URL}")
