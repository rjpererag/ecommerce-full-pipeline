import os
from dataclasses import dataclass

@dataclass
class SparkPostgresSettings:
    name: str = os.getenv("SPARK_APP_NAME", "my-spark-app")
    jdbc_url: str = os.getenv("SPARK_JDBC_URL", "jdbc:postgresql://localhost:5432/postgres")
    db_user: str = os.getenv("SPARK_DB_USER", "postgres")
    db_password: str = os.getenv("SPARK_DB_PASSWORD", "mypassword")
    db_driver: str = os.getenv("SPARK_DB_DRIVER", "org.postgresql.Driver")