"""
Spark job that using the run-aiflow-id and created_at columns partitions the data from the bronze layer
handles duplicates, normalize and loads the data to the silver layer dim and fact tables
"""

from .manager import SparkPostgresSettings, SparkPostgresManager
from .utils.structs import schema_del_payload
from pyspark.sql import DataFrame

from pyspark.sql import functions as F

def parse_payload(
        df: DataFrame,
):
    parsed_df = df.withColumn("data", F.from_json(F.col("payload"), schema_del_payload)) \
        .select("transaction_id", "data.*", "created_at")

    return parsed_df

def read_from_bronze(
        manager: SparkPostgresManager,
        airflow_run_id: str
) -> DataFrame:
    query = f"(SELECT * FROM bronze_layer WHERE airflow_run_id = '{airflow_run_id}') AS batch"
    return manager.read(query=query)


def _get_spark_manager(
        spark_settings: SparkPostgresSettings | None = None
) -> SparkPostgresManager:
    if not spark_settings:
        spark_settings = SparkPostgresSettings()
    manager = SparkPostgresManager(settings=spark_settings)
    manager.build_session()
    return manager


def run_spark_job(
        airflow_run_id: str,
        spark_settings: SparkPostgresSettings | None = None,
) -> None:
    print("Starting spark job")
    manager = _get_spark_manager(spark_settings=spark_settings)

    df_bronze = read_from_bronze(manager=manager, airflow_run_id=airflow_run_id)
    parsed_df = parse_payload(df=df_bronze)
    parsed_df.show(10)
