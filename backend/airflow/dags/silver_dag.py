"""
Dag to read from the bronze layer, perform transformations and load to silver layer
"""

import pendulum
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from spark.job_settings.bronze_to_silver_job import get_job_settings
from spark.bronze_to_silver_job import SparkPostgresSettings, SparkBronzeToSilverJob

# from ..plugins.spark.job_settings.bronze_to_silver_job import get_job_settings
# from ..plugins.spark.bronze_to_silver_job import SparkPostgresSettings, SparkBronzeToSilverJob

DB_CONN_ID = "my_postgres_db"

JOB_SETTINGS = get_job_settings()
MANAGER_SETTINGS = SparkPostgresSettings()


@task.short_circuit
def check_if_data_exists(rows_count):
    count = rows_count[0][0] if isinstance(rows_count, list) and isinstance(rows_count[0], tuple) else rows_count

    if count and int(count) > 0:
        print(f"Detected {count} records. Starting Spark processing...")
        return True
    print("No new records to process. Short-circuiting.")
    return False


@task
def trigger_spark_job(run_id):
    print(f"Initializing PySpark job for Airflow Run ID: {run_id}")
    job = SparkBronzeToSilverJob(
        manager_settings=MANAGER_SETTINGS,
        job_settings=JOB_SETTINGS,
    )
    results = job.run_job(airflow_run_id=run_id)

    print("JOB FINISHED")
    print("----------------------------------------")
    print(results)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze_to_silver"],
)
def bronze_to_silver():

    claim_sql = """
    WITH updated AS (
        UPDATE bronze_layer
        SET processed_status = 'processing',
        airflow_run_id = '{{ run_id }}'
        WHERE processed_status = 'to_process'
        RETURNING id
    )
    SELECT count(*) FROM updated;  
    """

    claim_batch = SQLExecuteQueryOperator(
        task_id="claim_pending_records",
        conn_id=DB_CONN_ID,
        sql=claim_sql,
        return_last=True,
        split_statements=False,
    )

    has_data = check_if_data_exists(claim_batch.output)
    has_data >> trigger_spark_job(run_id="{{ run_id }}")


bronze_to_silver()