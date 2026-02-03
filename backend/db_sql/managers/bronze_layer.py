from datetime import datetime, timedelta

from .credentials import DatabaseCredentials
from .manager import DBManager
from ..orms.bronze import Bronze


class BronzeManager(DBManager):

    def __init__(self, credentials: DatabaseCredentials):
        super().__init__(credentials=credentials)

    @staticmethod
    def create_record(
            transaction_details: dict,
            kafka_details: dict,
            processed_status: str,
            transaction_id_key: str = "transaction_id",
    ) -> Bronze:
        return Bronze(
            transaction_id = transaction_details[transaction_id_key],
            payload = transaction_details,
            processed_status = processed_status,
            kafka_metadata = kafka_details,
        )

    def add_transaction(
            self,
            transaction_details: dict,
            kafka_details: dict,
            processed_status: str,
    ):

        try:
            new_record = self.create_record(
                transaction_details=transaction_details,
                kafka_details=kafka_details,
                processed_status=processed_status,
            )

            with self.session() as session:
                session.add(new_record)
                session.commit()
                print(f"Transaction successfully loaded {transaction_details.get('id')}")
        except Exception as e:
            error = f"Transaction failed to be loaded in Bronze layer: {str(e)}"
            print(error) # TODO: Change for a log


    def query_by_date_diff(
            self,
            date_diff: datetime | None = None
    ) -> list[type[Bronze]] | None:

        if not date_diff:
            date_diff = datetime.now() - timedelta(days=1)

        try:
            with self.session() as session:
                results = session.query(Bronze).filter(Bronze.created_at >= date_diff).all()
                return results

        except Exception as e:
            error = f"Error querying Bronze layer: {str(e)}"
            print(error)  # TODO: Change for a log

