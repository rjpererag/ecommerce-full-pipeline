from backend.kafka.producer.service import Producer, ProducerSettings
from backend.db_sql.managers.bronze_layer import BronzeManager, DatabaseCredentials


def create_transactions(i: int) -> dict:
    return {
        "transaction_details": {
            "id": f"tx_98765_TEST_{i}",
            "amount": 100,
            "currency": "USD"
        },
        "kafka_details": {
            "topic": "sales",
            "partition": 1
        }
    }


def produce_messages():
    db_creds = DatabaseCredentials()
    manager = BronzeManager(credentials=db_creds)
    manager.start()

    payloads = [
        create_transactions(i=i)
        for i in range(100)
    ]

    producer = Producer()
    producer.create_producer()
    producer.produce(payload=payloads, multiple_messages=True)

def main():
    produce_messages()


if __name__ == '__main__':
    main()