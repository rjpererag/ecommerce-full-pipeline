# from pyspark.sql import functions as F
from pyspark.sql import types as T
from dataclasses import dataclass


# Columns to clean
__COLUMNS_TO_REMOVE_NULLS = ["payload", "data"]
__COLUMNS_TO_DROP_DUPLICATES = ["transaction_id"]

# CONFIG TO COLUMNS PARSING
__ROOT_COLS = {
    "transaction_id": ("transaction_id", T.StringType()),
}
__PAYLOAD_MAPPING_COLS = {
    "data.event_type": ("event_type", None),
    "data.event_version": ("event_version", None),
    "data.event_timestamp": ("event_timestamp_str", None),
    "data.customer.id": ("customer_id", T.StringType()),
    "data.order.status": ("order_status_id", T.IntegerType()),
    "data.order.payment_method": ("order_payment_method_id", T.IntegerType()),
    "data.order.currency": ("order_currency_id", T.IntegerType()),
    "data.order.subtotal": ("order_subtotal", T.DoubleType()),
    "data.order.tax_amount": ("order_tax_amount", T.DoubleType()),
    "data.order.shipping_amount": ("order_shipping_amount", T.DoubleType()),
    "data.order.discount_amount": ("order_discount_amount", T.DoubleType()),
    "data.order.total_amount": ("order_total_amount", T.DoubleType()),
    "data.customer": ("customer_info", None),
    "data.items": ("items", None)
}
__MAPPING_COLS = {**__ROOT_COLS, **__PAYLOAD_MAPPING_COLS}

__TRANSACTION_ITEMS_MAPPING = {
    "item.id": ("item_id", None),
    "transaction_id": ("transaction_id", None),
    "item.category": ("category", None),
    "item.unit_price": ("unit_price", T.IntegerType()),
    "item.quantity": ("quantity", T.IntegerType()),
    "item.discount": ("discount", T.DoubleType()),
    "item.cost": ("cost", T.DoubleType()),
}

# SILVER DB SCHEMA COLUMNS MAPPING

__SILVER_TABLE_COLS = {
    "dim_customers": {
        "customer_info.id": "id",
        "customer_info.type": "type",
        "customer_info.city": "city",
        "customer_info.country": "country",
    },
    "fact_transactions": {
        'transaction_id' : 'transaction_id',
        'event_type' : 'event_type',
        'event_version': 'event_version',
        'event_timestamp' : 'event_timestamp',
        'customer_id' : 'customer_id',
        'order_status_id' : 'order_status_id',
        'order_payment_method_id' : 'order_payment_method_id',
        'order_currency_id' : 'order_currency_id',
        'order_subtotal' : 'order_subtotal',
        'order_tax_amount' : 'order_tax_amount',
        'order_shipping_amount' : 'order_shipping_amount',
        'order_discount_amount' : 'order_discount_amount',
        'order_total_amount' : 'order_total_amount',
    },
    "fact_transaction_items": {
        'transaction_id' : 'transaction_id',
        'items': 'items',
    }
}

__DIM_CUSTOMERS_COLS = [col for col in __SILVER_TABLE_COLS["dim_customers"].values()]
__FACT_TRANSACTIONS_COLS = [col for col in __SILVER_TABLE_COLS["fact_transactions"].values()]
__FACT_TRANSACTION_ITEMS_COLS = [val[0] for val in __TRANSACTION_ITEMS_MAPPING.values()]
__FACT_TRANSACTION_ITEMS_COLS.append("id")


# UPSERT CONFIGURATION TO LOAD DATA TO DIMENSION AND FACT TABLES
__UPSERT_CONFIG = {
    "dim_customers": {
        "df": None,  # This will be reassigned after is selected
        "arguments": {
            "target_table": "dim_customers",
            "staging_table": None,# This will be reassigned after is selected,
            "cols_str": ", ".join(__DIM_CUSTOMERS_COLS),
            "conflict_column": "id"
        }
    },

    "fact_transactions": {
        "df": None,  # This will be reassigned after is selected
        "arguments": {
            "target_table": "fact_transactions",
            "staging_table": None,  # This will be reassigned after is selected,
            "cols_str": ", ".join(__FACT_TRANSACTIONS_COLS),
            "conflict_column": "transaction_id"
        },
    },

    "fact_transaction_items": {
         "df": None,  # This will be reassigned after is selected
         "arguments": {
             "target_table": "fact_transaction_items",
             "staging_table": None,  # This will be reassigned after is selected,
             "cols_str": ", ".join(__FACT_TRANSACTION_ITEMS_COLS),
             "conflict_column": "id"
         },
    }
}

@dataclass
class BronzeToSilverJobSettings:
    columns_to_remove_nulls: list[str]
    columns_to_drop_duplicates: list[str]
    columns_mapping: dict
    items_mapping: dict
    silver_tables_cols: dict
    upsert_config: dict


def get_job_settings(**kwargs) -> BronzeToSilverJobSettings:
    return BronzeToSilverJobSettings(
        columns_to_remove_nulls=kwargs.get("columns_to_remove_nulls", __COLUMNS_TO_REMOVE_NULLS),
        columns_to_drop_duplicates=kwargs.get("columns_to_drop_duplicates", __COLUMNS_TO_DROP_DUPLICATES),
        columns_mapping=kwargs.get("columns_mapping", __MAPPING_COLS),
        items_mapping = kwargs.get("items_mapping", __TRANSACTION_ITEMS_MAPPING),
        silver_tables_cols=kwargs.get("silver_tables_cols", __SILVER_TABLE_COLS),
        upsert_config=kwargs.get("upsert_config", __UPSERT_CONFIG),
    )