"""
Dag to read from the bronze layer, perform transformations and load to silver layer
"""

import pendulum
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from spark_jobs.job_settings.bronze_to_silver_job import get_job_settings
from spark_jobs.read_and_parse_job import ReadParseAndMoveJob
from spark_jobs.manager import SparkManager, SparkPostgresSettings
from postgres_jobs.manager.postgres import PostgresSettings
from postgres_jobs.db_handler import DBHandler

# from ..plugins.spark.job_settings.bronze_to_silver_job import get_job_settings
# from ..plugins.spark.read_and_parse_job import ReadParseAndMoveJob
# from ..plugins.spark.manager import SparkManager, SparkPostgresSettings

# from ..plugins.postgres_jobs.manager.postgres import PostgresSettings
# from ..plugins.postgres_jobs.db_handler import DBHandler

DB_CONN_ID = "my_postgres_db"

JOB_SETTINGS = get_job_settings()
SPARK_MANAGER_SETTINGS = SparkPostgresSettings()
POSTGRES_MANAGER_SETTINGS = PostgresSettings()


@task.short_circuit
def check_if_data_exists(rows_count):
    count = rows_count[0][0] if isinstance(rows_count, list) and isinstance(rows_count[0], tuple) else rows_count

    if count and int(count) > 0:
        print(f"Detected {count} records. Starting Spark processing...")
        return True
    print("No new records to process. Short-circuiting.")
    return False


@task
def trigger_read_parse_and_move_to_stage(run_id) -> dict:
    print(f"Initializing PySpark Read and Parse job for Airflow Run ID: {run_id}")
    job = ReadParseAndMoveJob(
        manager_settings=SPARK_MANAGER_SETTINGS,
        job_settings=JOB_SETTINGS,
    )
    results = job.run_job(airflow_run_id=run_id)

    print("JOB FINISHED")
    print("----------------------------------------")
    return results


@task
def print_results(results) -> None:
    print(results)


@task
def execute_final_upsert(upsert_configs: dict) -> dict:

    if not upsert_configs:
        return {}

    handler = DBHandler(settings=POSTGRES_MANAGER_SETTINGS)
    print(handler.manager.test_postgres_connection())
    results = handler.upsert_multiple_tables_from_staging(upsert_configs=upsert_configs)
    return results


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
    spark_results = trigger_read_parse_and_move_to_stage(run_id="{{ run_id }}")
    final_upsert = execute_final_upsert(upsert_configs=spark_results["upsert_configs"])

    has_data >> spark_results >> final_upsert >> print_results(final_upsert)
    # TO INCLUDE: ReadJob, ParserJob, MoveToStagingJob, UpsertJob (this is PostgreSQL focused)

bronze_to_silver()