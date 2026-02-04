from .manager import SparkPostgresSettings, SparkPostgresManager

def run_spark_job() -> None:
    spark_settings = SparkPostgresSettings()
    spark_manager = SparkPostgresManager(spark_settings)
    spark_manager.build_session()

    try:
        query = "(SELECT * FROM bronze_layer) AS subquery"
        df = spark_manager.read(query=query)
        df.show(10)
    except Exception as e:
        print(f"Error found: {str(e)}")
        print(spark_settings)