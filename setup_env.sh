#!/bin/bash

# Nombre del archivo de salida
ENV_FILE=".env"

echo "Creating environment variables to run in local. Location: $ENV_FILE"

# Escribir contenido al archivo .env
cat <<EOF > $ENV_FILE
# --- Kafka Configuration ---
KAFKA_TOPIC=my-new-test-topic

# --- PostgreSQL Configuration ---
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_PASSWORD=mypassword
POSTGRES_SSLMODE=disable
POSTGRES_VERSION=13

# --- Airflow Configuration ---
# Note: Using PostgreSQL Configuration
AIRFLOW_CONN_MY_POSTGRES_DB=postgres://postgres:mypassword@db:5432/postgres

# --- Spark Configuration ---
SPARK_JDBC_URL=jdbc:postgresql://db:5432/postgres
SPARK_DB_USER=postgres
SPARK_DB_PASSWORD=mypassword
SPARK_DB_DRIVER=org.postgresql.Driver
SPARK_DB_HOST=db
SPARK_DB_PORT=5432
SPARK_DB_NAME=postgres
EOF

echo "ENV_FILE created"