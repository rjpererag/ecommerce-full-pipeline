import psycopg2
from typing import Any
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import DatabaseError, OperationalError
from pyspark.sql import SparkSession, DataFrame
from .settings import SparkPostgresSettings

class SparkPostgresManager:

    def __init__(self, settings: SparkPostgresSettings):

        self.settings = settings
        self.spark_conn_properties = {
            "user": settings.db_user,
            "password": settings.db_password,
            "driver": settings.db_driver,
        }

        self.postgres_conn_properties = {
            "user": settings.db_user,
            "password": settings.db_password,
            "host": settings.db_host,
            "port": settings.db_port,
            "database": settings.db_name,
        }

        self.session: SparkSession | None = None
        self.postgres_pool: SimpleConnectionPool | None = None

    def build_conn_pool(
            self,
            min_conn: int,
            max_conn: int
    ) -> SimpleConnectionPool | None:
        try :
            self.postgres_pool = SimpleConnectionPool(
                min_conn, max_conn, **self.postgres_conn_properties
            )
        except Exception as e:
            print(f"Failed to build connection pool: {e}")
            return None


    def build_session(self) -> None:
        self.session = SparkSession.builder \
            .appName(self.settings.name) \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
            .getOrCreate()

    def read(self, query: str) -> DataFrame:
        df = self.session.read.jdbc(
            url=self.settings.jdbc_url,
            table=query,
            properties=self.spark_conn_properties,
        )
        return df

    def write_table(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        df.write \
            .format("jdbc") \
            .option("url",  self.settings.jdbc_url) \
            .option("dbtable",  table_name) \
            .option("user",  self.settings.db_user) \
            .option("password",  self.settings.db_password) \
            .option("driver",  self.settings.db_driver) \
            .mode(mode) \
            .save()

    def _validate_postgres_operation(
            self,
            query: str,
            operation_type: str,
    ) -> bool:

        if not self.postgres_pool:
            print("No connection pool available")
            return False

        if not operation_type.lower() in query.lower():
            print("Invalid operation type")
            return False

        return True

    def execute_insert_query(
            self,
            query: str,
            fetch: bool = False,
            row_count: bool = False,
    ) -> dict:

        conn = None
        result = {}

        if self._validate_postgres_operation(
                query=query, operation_type="INSERT"):

            try:
                conn = self.postgres_pool.getconn()
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()

                    if fetch:
                        result["fetch"] = cursor.fetchall()

                    if row_count:
                        result["row_count"] = cursor.rowcount

            except DatabaseError as e:
                print(f"Failed to execute insert query: {str(e)}")
                conn.rollback()
                raise

            finally:
                if conn:
                    self.postgres_pool.putconn(conn)

        return result

    def execute_select_query(self, query: str) -> Any:

        result = None
        conn = None

        if self._validate_postgres_operation(
                query=query, operation_type="SELECT"):

            try:
                conn = self.postgres_pool.getconn()
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()

            except DatabaseError as e:
                print(f"Failed to execute select query: {str(e)}")
                raise

            finally:
                if conn:
                    self.postgres_pool.putconn(conn)

        return result

    def drop_table(self, query: str) -> None:

        conn = None

        if self._validate_postgres_operation(
                query=query, operation_type="DROP"):

            try:
                conn = self.postgres_pool.getconn()
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()

            except DatabaseError as e:
                print(f"Failed to execute drop query: {str(e)}")
                conn.rollback()
                raise

            finally:
                if conn:
                    self.postgres_pool.putconn(conn)


    def test_postgres_connection(self):

        conn = None
        try:
            conn = self.postgres_pool.getconn()

            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
                result = cursor.fetchone()

                if result and result[0] == 1:
                    print("Connection test successful: Database is responsive.")
                    return True

        except OperationalError as e:
            print(f"Connection test failed (Network/Auth error): {e}")
        except Exception as e:
            print(f"An unexpected error occurred during connection test: {e}")

        finally:
            if conn:
                self.postgres_pool.putconn(conn)

        return False
