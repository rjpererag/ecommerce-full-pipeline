import json
import time

from kafka import KafkaProducer
from typing import Callable, Any

from .settings import ProducerSettings
from ..utils.logger import get_logger
from ..utils.utils import is_iter


class Producer:
    def __init__(self, settings: ProducerSettings | None = None):
        self.settings = self.__handle_settings(settings=settings)
        self.producer: KafkaProducer | None = None
        self.__logger = get_logger()

    @staticmethod
    def __handle_settings(settings: ProducerSettings) -> ProducerSettings:
        if not settings:
            return ProducerSettings()
        return settings

    def create_producer(self) -> None:
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.settings.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
        except Exception as e:
            self.__logger.error(f"No producer created. {str(e)}")

    @staticmethod
    def __get_message(payload) -> dict:
        return {
            # "timestamp": time.time(),
            "data": payload,
        }

    def __produce(self, payload, close_at_end: bool = False) -> None:
        message = self.__get_message(payload=payload)

        try:
            future = self.producer.send(
                topic=self.settings.topic,
                value=message,
            )
            record_metadata = future.get(timeout=10)
            self.__logger.info(f"""
            Message sent to {record_metadata.topic},
            partition: {record_metadata.partition},
            offset: {record_metadata.offset}
            """)

        except Exception as e:
            self.__logger.error(f"""
            Message not sent.
            Message was: {message}
            Error: {str(e)}""")

        finally:
            if close_at_end:
                self.__close_producer()


    def __close_producer(self) -> None:
        try:
            self.producer.flush()
            self.producer.close()
            self.__logger.info(f"Producer closed.")
        except Exception as e:
            self.__logger.error(f"Producer not closed. {str(e)}")

    def __produce_multiple (self, payloads: list | tuple | dict):
        if not is_iter(payloads):
            self.__logger.error("Messages not sent. Payloads are not iterable.")

        for p in payloads:
            self.__produce(p)

        self.__close_producer()


    def produce(self, payload, multiple_messages: bool = False) -> Any | None:
        if not self.producer:
            self.__logger.error("Producer not initialized")
            return None

        if multiple_messages:
            return self.__produce_multiple(payloads=payload)

        return self.__produce(payload=payload, close_at_end=True)
