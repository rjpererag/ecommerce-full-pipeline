FROM postgres:13

COPY ./schema/bronze_schema.sql /docker-entrypoint-initdb.d/01_bronze_schema.sql
COPY ./schema/silver_schema.sql /docker-entrypoint-initdb.d/02_silver_schema.sql
COPY ./schema/gold_schema.sql /docker-entrypoint-initdb.d/03_gold_schema.sql
COPY ./populate/populate_dimension_tables.sql /docker-entrypoint-initdb.d/04_db_populate_dimension_tables.sql