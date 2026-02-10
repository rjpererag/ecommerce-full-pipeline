from backend.kafka.consumer.service import Consumer, ConsumerSettings
from backend.db_sql.managers.bronze_layer import BronzeManager, DatabaseCredentials
from kafka.consumer.fetcher import ConsumerRecord

def init_manager() -> BronzeManager | None:
    db_creds = DatabaseCredentials(
        host="localhost",
    )
    manager = BronzeManager(credentials=db_creds)
    start_status = manager.start()

    if not start_status.get("error"):
        return manager
    return None

def callback(message: ConsumerRecord, manager: BronzeManager):
    data = message.value.get("data", {})
    kafka_details = {
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
    }

    manager.add_transaction(
        transaction_details=data.get("transaction_details"),
        kafka_details=kafka_details,
        processed_status=data.get("processed_status"),
    )

def main() -> None:

    manager = init_manager()
    if not manager:
        return

    settings = ConsumerSettings()
    consumer = Consumer(settings=settings)
    consumer.create_consumer()
    consumer.consume(callback=callback, manager=manager)


if __name__ == '__main__':
    main()