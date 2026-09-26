"""All settings, read from environment variables. On your laptop they come from .env
(loaded automatically). Inside docker, compose passes them in.  Owner: Person 1.
"""
import os

try:                                        # laptop scripts: read .env if python-dotenv is installed
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Kafka: laptop -> the 3 brokers' external ports; docker -> kafka:29092,kafka2:29092,kafka3:29092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")

# One database (the team's Supabase schema). Laptop default = the local postgres container.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@localhost:5432/ridehail")
# Payment service may be pointed elsewhere (chaos demo: payment DB unreachable).
PAYMENT_DATABASE_URL = os.getenv("PAYMENT_DATABASE_URL") or DATABASE_URL
# Supabase *transaction* pooler (port 6543) does not support prepared statements -> set "none".
_pt = os.getenv("DB_PREPARE_THRESHOLD", "none").lower()
DB_PREPARE_THRESHOLD = None if _pt in ("none", "") else int(_pt)
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "5"))

REDIS_URL = os.getenv("REDIS_URL", "redis://:redis-dev-password@localhost:6379/0")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))          # guardrail: retries before DLQ

# API keys (SecOps). Real values live in .env, never in git.
RIDER_API_KEY = os.getenv("RIDER_API_KEY", "dev-rider-key")
DRIVER_API_KEY = os.getenv("DRIVER_API_KEY", "dev-driver-key")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "dev-admin-key")
API_URL = os.getenv("API_URL", "http://localhost:8000")    # used by simulator / race test / locust

# Dispatch (Person 4)
OFFER_TIMEOUT_S = float(os.getenv("OFFER_TIMEOUT_S", "15"))
DRIVERS_PER_OFFER = int(os.getenv("DRIVERS_PER_OFFER", "3"))
MAX_DISPATCH_ROUNDS = int(os.getenv("MAX_DISPATCH_ROUNDS", "3"))
