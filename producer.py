from backend.kafka.producer.service import Producer, ProducerSettings
from backend.db_sql.managers.bronze_layer import BronzeManager, DatabaseCredentials
from fake_data_generator import Generator


def create_transactions(size: int) -> list[dict]:
    return Generator().generate_multiple_transactions(
        size=size, return_dict=True
    )

def produce_messages():
    db_creds = DatabaseCredentials()
    manager = BronzeManager(credentials=db_creds)
    manager.start()

    transactions = create_transactions(size=100)
    payloads = [
        {
            "transaction_details": transaction,
            "processed_status": "to_process",
        }
        for transaction in transactions
    ]

    producer = Producer()
    producer.create_producer()
    producer.produce(payload=payloads, multiple_messages=True)

def main():
    produce_messages()


if __name__ == '__main__':
    main()