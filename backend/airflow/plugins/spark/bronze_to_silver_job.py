"""
Spark job that using the run-aiflow-id and created_at columns partitions the data from the bronze layer
handles duplicates, normalize and loads the data to the silver layer dim and fact tables
"""

from .manager import SparkPostgresSettings, SparkPostgresManager
from .utils.structs import schema_del_payload
from .job_settings.bronze_to_silver_job import BronzeToSilverJobSettings
from pyspark.sql import DataFrame, Column

import re
import hashlib
from pyspark.sql import functions as F


class SparkBronzeToSilverJob:

    def __init__(
            self,
            manager_settings: SparkPostgresSettings | None = None,
            job_settings: BronzeToSilverJobSettings | None = None,
            **kwargs,

    ):
        self.manager_settings = manager_settings
        self.job_settings = job_settings
        self.kwargs = kwargs

        self.manager = SparkPostgresManager(self.manager_settings)
        self.manager.build_session()
        self.manager.build_conn_pool(
            min_conn=kwargs.get("min_conn", 2),
            max_conn=kwargs.get("min_conn", 3)
        )

    # STEP 1
    def read_from_bronze(
            self,
            airflow_run_id: str,
            **kwargs,
    ) -> DataFrame:
        table_name = kwargs.get("table_name", "bronze_layer")
        run_id_col = kwargs.get("run_id_col", "airflow_run_id")
        query = f"(SELECT * FROM {table_name} WHERE {run_id_col} = '{airflow_run_id}') AS batch"
        return self.manager.read(query=query)

    # STEP 2
    @staticmethod
    def expand_payload_to_cols(
            df: DataFrame,
            target_col: str = "payload",
            new_col: str = "data"
    ) -> DataFrame:
        return df.withColumn(
            new_col,
            F.from_json(F.col(target_col), schema_del_payload)
        )

    @staticmethod
    def __get_valid_cols(cols: list[str], df_cols: list[str]) -> list[str]:
        valid_cols = [col for col in cols if col in df_cols]
        return valid_cols

    # STEP 3.1
    def remove_nulls_from_cols(self, df: DataFrame, cols: list[str]) -> DataFrame:
        valid_cols = self.__get_valid_cols(cols, df.columns)

        if not valid_cols:
            return df

        for col in valid_cols:
            df = df.filter(F.col(col).isNotNull())

        return df

    # STEP 3.2
    def drop_duplicates_from_cols(self, df: DataFrame, cols: list[str]) -> DataFrame:
        cols_valid = self.__get_valid_cols(cols, df.columns)
        return df.dropDuplicates(cols_valid)

    # STEP 3
    def clean_df(self, df: DataFrame) -> DataFrame:
        df = self.remove_nulls_from_cols(df=df, cols=self.job_settings.columns_to_remove_nulls)
        df = self.drop_duplicates_from_cols(df=df, cols=self.job_settings.columns_to_drop_duplicates)
        return df

    @staticmethod
    def get_parsed_cols(mapping: dict) -> list[Column]:

        parsed_cols = []
        for col_path, (alias, col_type) in mapping.items():
            col_expr = F.col(col_path)

            if col_type:
                col_expr = col_expr.cast(col_type)

            parsed_cols.append(col_expr.alias(alias))

        return parsed_cols

    # STEP 4
    @staticmethod
    def parse(df: DataFrame, mapping: dict) -> DataFrame:

        def get_parsed_cols(_mapping: dict) -> list[Column]:
            cols = []
            for col_path, (alias, col_type) in _mapping.items():
                col_expr = F.col(col_path)

                if col_type:
                    col_expr = col_expr.cast(col_type)

                cols.append(col_expr.alias(alias))

            return cols

        parsed_cols = get_parsed_cols(_mapping=mapping)

        if parsed_cols:
            return df.select(parsed_cols)
        return df

    # STEP 5
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

    @staticmethod
    def _create_table_name(
            run_id: str,
            table_name: str | None = None,
            prefix: str = "stg"
    ) -> str:
        """
        Transforms an Airflow run_id into a valid PostgreSQL table name.
        Example: 'scheduled__2024-01-01T00:00:00+00:00' -> 'scheduled_2024_01_01t00_00_00_00_00'
        """
        name = run_id.lower()
        name = re.sub(r'[^a-z0-9]', '_', name)
        name = re.sub(r'_+', '_', name).strip('_')

        full_name = f"{prefix}_{name}"
        if table_name:
            full_name = f"{prefix}_{table_name}_{name}"

        if len(full_name) > 63:
            hash_suffix = hashlib.md5(run_id.encode()).hexdigest()[:8]
            full_name = f"{full_name[:54]}_{hash_suffix}"

        return full_name

    # STEP 6
    def move_to_stage_table(
            self,
            df: DataFrame,
            table_name: str,
            airflow_run_id: str,
    ) -> dict:

        staging_table_name = self._create_table_name(
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


    # STEP 6.1
    def move_multiple_tables_to_staging(
            self,
            dfs: dict[str, DataFrame],
            airflow_run_id: str
    ) -> dict:

        final_results = {}
        for df_name, df  in dfs.items():
            result = self.move_to_stage_table(
                df=df,
                table_name=df_name,
                airflow_run_id=airflow_run_id,
            )

            final_results = {**final_results, **result}

        return final_results

    # STEP 7
    def execute_upsert_from_staging_to_target(
            self, target_table: str, staging_table: str, cols_str: str, conflict_column: str
    ) -> dict:

        upsert_sql = f"""
INSERT INTO {target_table} ({cols_str})
SELECT {cols_str} FROM {staging_table}
ON CONFLICT ({conflict_column}) DO NOTHING
RETURNING {conflict_column};
        """

        result = {
            target_table: {
                "staging_table": staging_table,
                "cols_str": cols_str,
                "conflict_column": conflict_column,
                "query": upsert_sql,
            }
        }

        try:

            upsert_result = self.manager.execute_insert_query(
                query=upsert_sql,
                row_count=True,
                fetch=True,
            )
            result[target_table] = {
                **result[target_table],
                "status": "ok",
                "upsert_result": {**upsert_result},
            }

        except Exception as e:
            result[target_table] = {
                **result[target_table],
                "status": "failed",
                "error": str(e),
            }

        return result


    @staticmethod
    def build_upsert_config(
            upsert_config: dict, dfs: dict, staging_results: dict
    ) -> dict:

        for table_name, config in upsert_config.items():
            config["df"] = dfs.get(table_name)
            config["arguments"]["staging_table"] = staging_results.get(table_name, {}).get("staging_table_name")

        return upsert_config

    # STEP 7.1
    def upsert_multiple_tables_from_staging(
            self, upsert_configs: dict
    ) -> dict:

        results = {}
        for table_name, config in upsert_configs.items():
            print(f"Handling: {table_name}")
            upsert_results = self.execute_upsert_from_staging_to_target(
                **config["arguments"],
            )
            results = {**results, **upsert_results}
        return results

    @staticmethod
    def __explode_df(
            df: DataFrame,
            col_to_explode: str,
            alias: str,
            cols_to_keep: list[str],
    ) -> DataFrame:
        cols_to_keep = [F.col(name) for name in cols_to_keep]
        exploded_df = df.select(
            *cols_to_keep,
            F.explode(col_to_explode).alias(alias),
        )
        return exploded_df


    def explode_and_parse_items(
            self,
            df: DataFrame,
    ) -> DataFrame | None:

        if df.isEmpty():
            return None

        exploded_df = self.__explode_df(
            df=df,
            col_to_explode="items",
            alias="item",
            cols_to_keep=["transaction_id"]
        )

        parsed_df = self.parse(df=exploded_df, mapping=self.job_settings.items_mapping)

        final_df = parsed_df.select(
            F.concat(F.col("transaction_id"), F.lit("-"), F.col("item_id")).alias("id"),
            *[F.col(val[0]) for val in self.job_settings.items_mapping.values()],
        )

        return final_df


    @staticmethod
    def change_to_timestamp(
            df: DataFrame,
            col_to_change: str,
            alias: str,
    ) -> DataFrame:

        cols_to_keep = [F.col(col) for col in df.columns]
        new_df = df.select(
            *cols_to_keep,
            F.to_timestamp(F.col(col_to_change)).alias(alias)
        )
        return new_df


    def select_clean_and_parse_from_bronze(self, airflow_run_id: str) -> dict[str, DataFrame | None]:
        bronze_df = self.read_from_bronze(airflow_run_id=airflow_run_id)
        expanded_df = self.expand_payload_to_cols(df=bronze_df)
        clean_df = self.clean_df(df=expanded_df)
        parsed_df = self.parse(df=clean_df, mapping=self.job_settings.columns_mapping)
        parsed_df = self.change_to_timestamp(df=parsed_df, col_to_change="event_timestamp_str", alias="event_timestamp")

        selected_dfs = {
            df_name: self.get_df_from_col_selection(
                df=parsed_df,
                df_cols=cols,
                distinct=True,
            )
            for df_name, cols in self.job_settings.silver_tables_cols.items()
        }

        return selected_dfs


    def drop_staging_tables(self, upsert_results: dict) -> dict:

        for table, results in upsert_results.items():
            print(f"Dropping: {table}")

            if results.get("status") != "ok":
                msg = f"""
                Skipping, failed staging table.
                Table name: {table}
                Staging Table: {results.get("staging_table")}
                """
                print(msg)
                results["drop_staging_table"] = {"msg": msg, "dropped": False}

            try:
                query = f"DROP TABLE IF EXISTS {results.get('staging_table')};"
                self.manager.drop_table(query=query)
                msg = f"{results.get('staging_table')} dropped"
                results["drop_staging_table"] = {"msg": msg, "dropped": True}

            except Exception as e:
                msg = f"Error dropping {results.get('staging_table')}. {str(e)}"
                results["drop_staging_table"] = {"msg": msg, "dropped": False}

        return upsert_results


    def run_job(self, airflow_run_id)-> dict:

        selected_dfs = self.select_clean_and_parse_from_bronze(airflow_run_id=airflow_run_id)
        items_df = self.explode_and_parse_items(df=selected_dfs.get('fact_transaction_items'))

        selected_dfs['fact_transaction_items'] = items_df

        staging_dfs_results = self.move_multiple_tables_to_staging(
            dfs=selected_dfs,
            airflow_run_id=airflow_run_id,
        )

        upserts_config = self.build_upsert_config(
            upsert_config=self.job_settings.upsert_config,
            dfs=selected_dfs,
            staging_results=staging_dfs_results,
        )

        upsert_results = self.upsert_multiple_tables_from_staging(
            upsert_configs=upserts_config,
        )

        results = self.drop_staging_tables(upsert_results=upsert_results)

        return results
