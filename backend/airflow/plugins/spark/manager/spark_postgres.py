from pyspark.sql import SparkSession, DataFrame
from .settings import SparkPostgresSettings

class SparkPostgresManager:

    def __init__(self, settings: SparkPostgresSettings):

        self.settings = settings
        self.conn_properties = {
            "user": settings.db_user,
            "password": settings.db_password,
            "driver": settings.db_driver,
        }

        self.session: SparkSession | None = None

    def build_session(self) -> None:
        self.session = SparkSession.builder \
            .appName(self.settings.name) \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
            .getOrCreate()

    def read(self, query: str) -> DataFrame:
        df = self.session.read.jdbc(
            url=self.settings.jdbc_url,
            table=query,
            properties=self.conn_properties,
        )
        return df