"""
Spark job that using the run-aiflow-id and created_at columns partitions the data from the bronze layer
handles duplicates, normalize and loads the data to the silver layer dim and fact tables
"""

from .manager.spark_postgres import SparkManager, SparkPostgresSettings
from .utils.structs import schema_del_payload
from .utils.spark import *
from .job_settings.bronze_to_silver_job import BronzeToSilverJobSettings
from pyspark.sql import DataFrame

from pyspark.sql import functions as F


class ReadParseAndMoveJob:

    def __init__(
            self,
            manager_settings: SparkPostgresSettings,
            job_settings: BronzeToSilverJobSettings | None = None,
            **kwargs,

    ):
        self.job_settings = job_settings
        self.kwargs = kwargs
        self.manager_settings = manager_settings
        self.manager = SparkManager(settings=self.manager_settings)
        self.manager.build_session()


    def read_from_bronze(
            self,
            airflow_run_id: str,
            **kwargs,
    ) -> DataFrame:
        table_name = kwargs.get("table_name", "bronze_layer")
        run_id_col = kwargs.get("run_id_col", "airflow_run_id")
        query = f"(SELECT * FROM {table_name} WHERE {run_id_col} = '{airflow_run_id}') AS batch"
        return self.manager.read(query=query)

    def clean_df(self, df: DataFrame) -> DataFrame:
        df = remove_nulls_from_cols(df=df, cols=self.job_settings.columns_to_remove_nulls)
        df = drop_duplicates_from_cols(df=df, cols=self.job_settings.columns_to_drop_duplicates)
        return df

    @staticmethod
    def parse(df: DataFrame, mapping: dict) -> DataFrame:
        parsed_cols = get_parsed_cols(mapping=mapping)

        if parsed_cols:
            return df.select(parsed_cols)
        return df

    @staticmethod
    def get_df_from_col_selection(
            df: DataFrame,
            df_cols: dict,
            distinct: bool = True,
    ) -> DataFrame | None:

        cols_to_select = [
            F.col(col_path).alias(alias)
            for col_path, alias in df_cols.items()
        ]

        if not cols_to_select:
            return None

        if not distinct:
            return df.select(*cols_to_select)
        return df.select(*cols_to_select).distinct()

    def explode_and_parse_items(
            self,
            df: DataFrame,
    ) -> DataFrame | None:

        try:
            if df.isEmpty():
                return None

            exploded_df = explode_df(
                df=df,
                col_to_explode="items",
                alias="item",
                cols_to_keep=["transaction_id", "event_timestamp"]
            )

            parsed_df = self.parse(df=exploded_df, mapping=self.job_settings.items_mapping)

            final_df = parsed_df.select(
                F.concat(F.col("transaction_id"), F.lit("-"), F.col("item_id")).alias("id"),
                *[F.col(val[0]) for val in self.job_settings.items_mapping.values()],
            )

            return final_df
        except Exception as e:
            raise Exception(f"explode_and_parse_items error: {str(e)}")

    def select_clean_and_parse_from_bronze(self, airflow_run_id: str) -> dict[str, DataFrame | None]:
        try:
            bronze_df = self.read_from_bronze(airflow_run_id=airflow_run_id)
            expanded_df = expand_json_to_col(df=bronze_df, schema=schema_del_payload)
            clean_df = self.clean_df(df=expanded_df)
            parsed_df = self.parse(df=clean_df, mapping=self.job_settings.columns_mapping)
            parsed_df = change_to_timestamp(df=parsed_df, col_to_change="event_timestamp_str", alias="event_timestamp")

            selected_dfs = {
                df_name: self.get_df_from_col_selection(
                    df=parsed_df,
                    df_cols=cols,
                    distinct=True,
                )
                for df_name, cols in self.job_settings.silver_tables_cols.items()
            }
            return selected_dfs

        except Exception as e:
            raise Exception(f"select_clean_and_parse_from_bronze error: {str(e)}")


    def move_to_stage_table(
            self,
            df: DataFrame,
            table_name: str,
            airflow_run_id: str,
    ) -> dict:

        staging_table_name = create_table_name(
            run_id=airflow_run_id, table_name=table_name, prefix="stg"
        )

        result = {
            table_name: {
                "run_id": airflow_run_id,
                "staging_table_name": staging_table_name,
                }
            }

        try:
            self.manager.write_table(
                df=df.distinct(),
                table_name=staging_table_name,
                mode="overwrite",
            )

            result[table_name] = {
                **result[table_name],
                "status": "ok",
                "mode": "overwrite",
            }

        except Exception as e:
            result[table_name] = {
                **result[table_name],
                "status": "failed",
                "error": str(e),
            }

        return result

    def move_multiple_tables_to_staging(
            self,
            dfs: dict[str, DataFrame],
            airflow_run_id: str
    ) -> dict:

        try:
            final_results = {}
            for df_name, df  in dfs.items():
                result = self.move_to_stage_table(
                    df=df,
                    table_name=df_name,
                    airflow_run_id=airflow_run_id,
                )

                final_results = {**final_results, **result}

            return final_results
        except Exception as e:
            raise Exception(f"move_multiple_tables_to_staging error: {str(e)}")


    @staticmethod
    def build_upsert_config(
            upsert_config: dict, dfs: dict, staging_results: dict
    ) -> dict:
        try:
            for table_name, config in upsert_config.items():
                config["df"] = dfs.get(table_name)
                config["arguments"]["staging_table"] = staging_results.get(table_name, {}).get("staging_table_name")

            return upsert_config
        except Exception as e:
            raise Exception(f"build_upsert_config error: {str(e)}")

    def run_job(self, airflow_run_id)-> dict:

        selected_dfs = self.select_clean_and_parse_from_bronze(airflow_run_id=airflow_run_id)
        items_df = self.explode_and_parse_items(df=selected_dfs.get('fact_transaction_items'))
        selected_dfs['fact_transaction_items'] = items_df

        staging_dfs_results = self.move_multiple_tables_to_staging(
            dfs=selected_dfs,
            airflow_run_id=airflow_run_id,
        )

        upsert_config = self.build_upsert_config(
            upsert_config=self.job_settings.upsert_config,
            dfs=staging_dfs_results,
            staging_results=staging_dfs_results,
        )

        return {
            "staging_results": staging_dfs_results,
            "upsert_configs": upsert_config,
        }
