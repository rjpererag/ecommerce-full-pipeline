import random


def create_fake_customers_pool(
        size: int,
        customer_type : list | None = None,
        locations: list[dict] | None = None,
) -> list[dict]:

    if not customer_type:
        customer_type = ["new", "returning"]

    if not locations:
        locations = {
            "Spain": {"code": "ES", "cities": ["Madrid", "Barcelona"]},
            "Germany": {"code": "DE", "cities": ["Berlin", "Frankfurt"]},
            "Italy": {"code": "IT", "cities": ["Milan", "Rome"]},
        }

    def get_customer(index: int) -> dict:
        l = random.choice(list(locations.keys()))
        location = locations[l]
        return {
            "id": f"cust_{index + 1}",
            "type": random.choice(customer_type),
            "country": location["code"],
            "city": random.choice(location["cities"]),
        }

    return [get_customer(index=i) for i in range(size)]



def create_fake_products_pool(
        size: int,
        category_type : list | None = None,
        min_price: float = 0.0,
        max_price: float = 100.0,
) -> list[dict]:

    if not category_type:
        category_type = ["electronics", "home", "sports", "fashion", "automotive", "fashion"]

    def get_product(index: int) -> dict:
        unit_price = random.uniform(min_price, max_price)

        return {
            "id": f"prod_{index + 1}",
            "category": random.choice(category_type),
            "unit_price": round(unit_price, 2),
            "cost": round(unit_price * 0.3, 2),
        }

    return [get_product(index=i) for i in range(size)]

