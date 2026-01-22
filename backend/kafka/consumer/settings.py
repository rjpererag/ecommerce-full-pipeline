# settings.py
from dataclasses import dataclass
from decouple import config

@dataclass
class ConsumerSettings:
    topics: str = config("KAFKA_TOPIC", cast=str, default="test-topic")
    auto_offset_reset: str = config("KAFKA_AUTO_OFFSET_RESET", cast=str, default="earliest")
    bootstrap_server: str = config("KAFKA_BOOTSTRAP_SERVER", cast=str, default="localhost:9092")
    enable_auto_commit: bool = config("KAFKA_ENABLE_AUTO_COMMIT", cast=str, default=True)
    group_id: str = config("KAFKA_GROUP_ID", cast=str, default="test-topic")
