"""
Dag to read from the silver layer, perform transformations to get KPIs and load to golden layer
"""

import pendulum
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


DB_CONN_ID = "my_postgres_db"

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["gold", "sales_perfomance", "items_category_performance"],
)
def silver_to_gold():

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

    sales_performance >> items_category_performance

silver_to_gold()
