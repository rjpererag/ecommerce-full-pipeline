from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DecimalType, ArrayType, IntegerType)


schema_del_payload = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_version", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("customer", StructType([
        StructField("id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("type", StringType(), True),
        StructField("country", StringType(), True)
    ]), True),
    StructField("order", StructType([
        StructField("status", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("shipping_amount", DoubleType(), True)
    ]), True),
    StructField("items", ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("cost", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("discount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True)
    ])), True)
])