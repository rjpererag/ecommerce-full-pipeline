from backend.kafka.producer.service import Producer, ProducerSettings
from backend.db_sql.managers.bronze_layer import BronzeManager, DatabaseCredentials
from .fake_data_generator import Generator


def create_transactions(
        size: int,
        n_days_before: int = 0,
        random_days_before: list[int] | None = None,
) -> list[dict]:
    return Generator().generate_multiple_transactions(
        size=size, return_dict=True, n_days_before=n_days_before, random_day_before=random_days_before
    )

def produce_messages():
    db_creds = DatabaseCredentials()
    manager = BronzeManager(credentials=db_creds)
    manager.start()

    total_days_before = 31
    random_days_before = [i * -1 for i in range(0, total_days_before)]

    transactions = create_transactions(size=1000, random_days_before=random_days_before)
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