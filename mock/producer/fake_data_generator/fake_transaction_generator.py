import random
from dataclasses import asdict
from datetime import datetime, timedelta
import uuid

from .dataclasses import Customer, Order, Item, Transaction
from .utils import create_fake_customers_pool, create_fake_products_pool


class FakeTransactionGenerator:

    def __init__(self):
        self.customer_pool: list = create_fake_customers_pool(size=100)
        self.products_pool: list = create_fake_products_pool(size=100)
        self.payment_methods = [1, 2, 3]


    @staticmethod
    def _handle_return_type(return_dict: bool, transaction: Transaction) -> Transaction | dict:
        if return_dict:
            return asdict(transaction)
        return transaction

    @staticmethod
    def _generate_transaction_id(
            timestamp: str
    ):
        return f"txn_{str(uuid.uuid4())}_{timestamp}"

    @staticmethod
    def __get_timestamp(n_days_before: int = 0) -> dict:
        timestamp = datetime.now()
        if n_days_before and (n_days_before < 0):
            timestamp += timedelta(days=n_days_before)

        now_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return {
            "compiled": now_str.replace("-", " ").replace(":", " ").replace(" ", ""),
            "extend": now_str.replace(" ", "T")
        }

    def create_items_list(self, n_items: int = 1) -> list[Item]:

        def build_item(item: dict) -> Item:
            discount_choice = random.choices([True, False])
            item_placed = {
                **item,
                "quantity": random.randint(1, 10),
                "discount": round(item["unit_price"] * 0.10, 2) if discount_choice else 0,
            }
            return Item(**item_placed)

        random_items = random.choices(self.products_pool, k=n_items)
        return [build_item(item=item) for item in random_items]

    def get_order_details(self, item_list: list[Item]) -> Order:

        def process_items_list(items: list[Item]) -> dict:
            discount_choice = random.choices([True, False])
            subtotal = round(sum([item.unit_price * item.quantity for item in items]), 2)
            tax_amount = round(subtotal * 0.10, 2)
            ship_amount  = round(subtotal * 0.02, 2)
            discount_amount = round(subtotal * 0.10, 2) if discount_choice else 0
            total_amount = round(subtotal + tax_amount + ship_amount - discount_amount, 2)
            return {
                "subtotal": subtotal,
                "tax_amount": tax_amount,
                "shipping_amount": ship_amount,
                "discount_amount": discount_amount,
                "total_amount": total_amount,
            }

        order = {
            **process_items_list(items=item_list),
            "payment_method": random.choice(self.payment_methods),
            "status": 5,
            "currency": 1,
        }

        return Order(**order)

    def generate_single_transaction(
            self,
            return_dict: bool = False,
            number_of_items: int = 0,
            n_days_before: int = 0,
    ) -> Transaction | dict:

        if number_of_items == 0:
            number_of_items = random.randint(1, 10)

        timestamp = self.__get_timestamp(n_days_before=n_days_before)
        transaction_id = self._generate_transaction_id(timestamp=timestamp["compiled"])

        items = self.create_items_list(n_items=number_of_items)
        order = self.get_order_details(item_list=items)

        customer_transaction = Transaction(
            transaction_id=transaction_id,
            event_type="purchase",
            event_version="1.0",
            event_timestamp=timestamp["extend"],
            customer = Customer(**random.choice(self.customer_pool)),
            items=items,
            order=order,
        )

        return self._handle_return_type(return_dict, customer_transaction)


    def _generate_multiple_transactions_static_day(
            self, size: int,
            number_of_items: int = 0,
            return_dict: bool = False,
            n_days_before: int = 0,
    ) -> list[Transaction | dict]:
        transactions = []
        for _ in range(size):
            transactions.append(
                self.generate_single_transaction(
                    number_of_items=number_of_items, return_dict=return_dict, n_days_before=n_days_before
                )
            )
        return transactions


    def _generate_multiple_transactions_random_day(
            self, size: int,
            number_of_items: int = 0,
            return_dict: bool = False,
            random_day_before: list[int] = 0,
    ) -> list[Transaction | dict]:
        transactions = []
        for _ in range(size):

            n_days_before = random.choices(random_day_before)[0]
            if n_days_before > 0:
                n_days_before *= -1

            transactions.append(
                self.generate_single_transaction(
                    number_of_items=number_of_items,
                    return_dict=return_dict,
                    n_days_before=n_days_before,
                )
            )
        return transactions


    def generate_multiple_transactions(
            self, size: int,
            number_of_items: int = 0,
            return_dict: bool = False,
            n_days_before: int = 0,
            random_day_before: list[int] | None = None
    ) -> list[Transaction | dict]:

        if not random_day_before:
            return self._generate_multiple_transactions_static_day(
                size=size,
                number_of_items=number_of_items,
                return_dict=return_dict,
                n_days_before=n_days_before,
            )

        return self._generate_multiple_transactions_random_day(
            size=size,
            number_of_items=number_of_items,
            return_dict=return_dict,
            random_day_before=random_day_before,
        )
