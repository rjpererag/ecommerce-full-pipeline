from .manager.postgres import PostgresManager, PostgresSettings


class DBHandler:

    def __init__(self, settings: PostgresSettings, **kwargs):
        self.manager = PostgresManager(settings=settings)
        self.manager.build_conn_pool(
            min_conn=kwargs.get("min_conn", 2),
            max_conn=kwargs.get("min_conn", 3)
        )

        test_result = self.manager.test_postgres_connection()
        print(test_result)

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


