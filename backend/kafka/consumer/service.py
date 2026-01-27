# service.py
import json
from kafka import KafkaConsumer
from typing import Callable, Any

from .settings import ConsumerSettings
from ..utils.logger import get_logger

class Consumer:
    def __init__(self, settings: ConsumerSettings | None = None):
        self.settings = self.__handle_settings(settings=settings)
        self.consumer: KafkaConsumer | None = None
        self.__logger = get_logger()

    @staticmethod
    def __handle_settings(settings: ConsumerSettings) -> ConsumerSettings:
        if not settings:
            return ConsumerSettings()
        return settings

    def create_consumer(self) -> None:
        try:
            self.consumer = KafkaConsumer(
                self.settings.topics,
                bootstrap_servers=self.settings.bootstrap_server,
                auto_offset_reset=self.settings.auto_offset_reset,
                enable_auto_commit=self.settings.enable_auto_commit,
                group_id=self.settings.group_id,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
        except Exception as e:
            self.__logger.error(f"No consumer created. {str(e)}")

    def _execute_callback(self, callback: Callable, *args, **kwargs):
        self.__logger.info("Waiting for messages")
        for message in self.consumer:
            callback(message, *args, **kwargs)

    def __consume(self, callback: Callable, *args, **kwargs) -> Any | None:
        try:
            return self._execute_callback(callback=callback, *args, **kwargs)
        except KeyboardInterrupt:
            self.__logger.info("\nShutting down consumer...")
        finally:
            self.consumer.close()
            self.__logger.info("Consumer closed")

    def consume(self, callback: Callable, *args, **kwargs) -> Any | None:
        if not self.consumer:
            self.__logger.error("Consumer not initialized")
            return None

        return self.__consume(callback=callback, *args, **kwargs)

