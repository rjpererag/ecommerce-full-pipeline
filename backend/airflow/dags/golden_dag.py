"""
Dag to read from the silver layer, perform transformations to get KPIs and load to golden layer
"""

import pendulum
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


DB_CONN_ID = "my_postgres_db"
fact_transactions_table = "fact_transactions"

@task.short_circuit
def check_if_data_exists(rows_count):
    count = rows_count[0][0] if isinstance(rows_count, list) and isinstance(rows_count[0], tuple) else rows_count

    if count and int(count) > 0:
        print(f"Detected {count}. Inserting in data marts")
        return True
    print("No new records to process. Short-circuiting.")
    return False


@dag(
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "sales_perfomance", "items_category_performance"],
)
def silver_to_gold():

    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze_dag',
        external_dag_id='bronze_to_silver',
        external_task_id=None,
        execution_delta=timedelta(hours=2),
        timeout=3600,
        mode='reschedule'
    )

    fact_transactions_row_count = SQLExecuteQueryOperator(
        task_id="claim_pending_records",
        conn_id=DB_CONN_ID,
        sql=f"SELECT count(*) FROM {fact_transactions_table} WHERE event_timestamp::date = '{{{{ ds }}}}'::date",
        return_last=True,
        split_statements=False,
    )

    has_data = check_if_data_exists(fact_transactions_row_count.output)

    sales_performance = SQLExecuteQueryOperator(
        task_id="sales_performance_mart",
        conn_id=DB_CONN_ID,
        sql="sql/sales_performance.sql",
        split_statements=True,
        do_xcom_push=True,
    )

    items_category_performance = SQLExecuteQueryOperator(
        task_id="items_category_performance_mart",
        conn_id=DB_CONN_ID,
        sql="sql/items_category_performance.sql",
        split_statements=True,
        do_xcom_push=True,
    )

    wait_for_bronze >> has_data >> sales_performance >> items_category_performance

silver_to_gold()
