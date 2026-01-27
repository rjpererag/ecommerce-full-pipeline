# settings.py
from dataclasses import dataclass
from decouple import config

@dataclass
class ProducerSettings:
    topic: str = config("KAFKA_TOPIC", cast=str, default="test-topic")
    bootstrap_server: str = config("KAFKA_BOOTSTRAP_SERVER", cast=str, default="localhost:9092")
