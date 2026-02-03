from dataclasses import dataclass

@dataclass
class Customer:
    id: str
    type: str
    country: str
    city: str

@dataclass
class Order:
    status: str
    payment_method: str
    currency: str
    subtotal: float
    tax_amount: float
    shipping_amount: float
    discount_amount: float
    total_amount: float

@dataclass
class Item:
    id: str
    category: str
    unit_price: float
    quantity: float
    discount: float
    cost: float


@dataclass
class Transaction:
    transaction_id: str
    event_type: str
    event_version: str
    event_timestamp: str
    customer: Customer
    order: Order
    items: list[Item]
