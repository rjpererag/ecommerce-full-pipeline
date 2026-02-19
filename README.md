# E-commerce Data Pipeline using Medallion Architecture and Decoupling services

This project is a demonstration on how to build a full data pipeline for an e-commerce as use case. The pipeline handles
the data lifecycle from generation to consumption using industry standards technologies like Kafka, Airflow and Spark. 

The architecture is designed in order to be able of fulfilling high volume of traffic, so a decoupled service to handle
multiple requests is built guaranteeing real-time and distributed logic for data ingestion. Data transformation on the
other hand is handled by dedicated data pipelines to execute tasks of cleaning, validation, transformation and normalization.

During all the data lifecycle a Medallion Architecture is applied to guarantee data persistence and traceability, using
a Bronze layer for raw data, Silver layer for clean, transformed and normalized data, and a Golden layer as the business logic
storing strategic information.

## Objectives

- Build a scalable real-time ingestion service using apache Kafka.
- Design a Data Warehouse where the medallion layers will live using PostgreSQL.
- Orchestrate scalable cleaning, validation and transformation pipelines using apache Airflow.
- Create Spark job/s to handle high volume of data transformation.
- Design a dynamic dashboard using to visualize strategic information.

## Technology Stack

- Apache Airflow
- Apache Grafana
- Apache Spark
- Docker
- Python

## Project structure

```
├── README.md
├── backend
│   ├── airflow
│   │   ├── Dockerfile
│   │   ├── dags
│   │   │   ├── golden_dag.py
│   │   │   ├── silver_dag.py
│   │   │   └── sql
│   │   │       ├── items_category_performance.sql
│   │   │       ├── sales_performance.sql
│   │   │       └── select_sales.sql
│   │   ├── plugins
│   │   │   ├── postgres_jobs
│   │   │   │   ├── db_handler.py
│   │   │   │   ├── manager
│   │   │   │   │   └── postgres.py
│   │   │   │   └── utils
│   │   │   ├── run_spark_bronze_to_silver.py
│   │   │   └── spark_jobs
│   │   │       ├── job_settings
│   │   │       │   └── bronze_to_silver_job.py
│   │   │       ├── manager
│   │   │       │   ├── settings.py
│   │   │       │   └── spark_postgres.py
│   │   │       ├── read_and_parse_job.py
│   │   │       └── utils
│   │   │           ├── spark.py
│   │   │           └── structs.py
│   │   └── requirements.txt
│   ├── db_sql
│   │   ├── database
│   │   │   ├── Dockerfile
│   │   │   ├── populate
│   │   │   │   └── populate_dimension_tables.sql
│   │   │   └── schema
│   │   │       ├── bronze_schema.sql
│   │   │       ├── gold_schema.sql
│   │   │       └── silver_schema.sql
│   │   ├── managers
│   │   │   ├── bronze_layer.py
│   │   │   ├── credentials.py
│   │   │   └── manager.py
│   │   └── orms
│   │       └── bronze.py
│   ├── grafana
│   │   └── provisioning
│   │       ├── dashboards
│   │       │   ├── dashboard_config.yaml
│   │       │   └── sales_performance_v1.json
│   │       └── datasources
│   │           └── datasource.yaml
│   └── kafka
│       ├── consumer
│       │   ├── service.py
│       │   └── settings.py
│       ├── producer
│       │   ├── service.py
│       │   └── settings.py
│       └── utils
│           ├── logger.py
│           └── utils.py
├── docker-compose-tools.yml
├── docker-compose.yaml
├── main.py
├── mock
│   ├── consumer
│   │   ├── Dockerfile
│   │   └── consumer.py
│   └── producer
│       ├── Dockerfile
│       ├── fake_data_generator
│       │   ├── dataclasses.py
│       │   ├── fake_transaction_generator.py
│       │   └── utils.py
│       └── producer.py
└── requirements.txt
```
Following the project structure, the backend is isolating the logic per service used, using dedicated directories for:
- **backend/**
  - **airflow**: in this service all the orchestration logic is designed, where external services or scripts used by
  the pipelines are within the plugins directory. A Dockerfile is created in this directory for dependencies handling.
  - **db_sql**: the Data Warehouse definition (schemas, populate scripts) are handled by this service using PostgreSQL 
  as DB engine. The ORMs used to handle the insertion to the Bronze Layer are handled by this service as well.
  - **grafana**: to handle the dashboard creation, handling data source connection and panels definition.
  - **kafka**: to create the Consumer and Producer logic following a Pub/Sub pattern.
- **mock/**: this special directory is used to mimic the e-commerce activity (creating sales data) as the Data Producer,
triggering the entire pipeline.

## Architecture

As mentioned before, this use case follows a medallion and decoupling architecture to handle data extraction,
transformation and loading (ETL) operations, while guaranteeing a service's single responsibility scope and data
persistence.

To accomplish this the architecture is divided in:

- **Ingest**: handled by a Pub/Sub service leveraging on Kafka for a real-time and scalable solution to feed the Bronze Layer,
where data persistence and fidelity are the main priority.
- **Transform**: handled by Airflow to orchestrate dedicated pipelines to perform validation, cleaning and transformation
operations in order to normalize the data into a Star Schema, so complex queries can be performed to provide business answers. 
Considering a high volume of data, a Spark job is designed within this step for a distributed processing.
- **Load**: this operations will be handled by Airflow as well to guarantee as are part of the orchestrated pipelines to
move data between layers based on the data maturity.


As mentioned, the architecture is using PostgreSQL, as data warehouse, where the medallion architecture will live in.
Following this, the warhouse will be divided in:

- **Bronze Layer**: where the raw data will arreive from the Pub/Sub service, no data transformation is applied in this
layer to guarantee persistence and fidelity. On the other hand, this layer will be monitored by an Airflow DAG to trigger the
transformation logic.
- **Silver Layer**: in this layer the data will be normalized into a Star Schema so complex queries can be performed to get
business answers. The priority in this layer includes: data duplication and null data handling.
- **Golden Layer**: this is the final and consumption layer, where strategic business information will live in, this layer
is conceptualized as Data Marts with optimized tables to retrieve reliable information, for example, sales performance
evolution through time.


## Getting Started

### Environment variables
Each service container requires important environment variables to be declared, for this a .env file can be created to
store this data.

Below is .env example to use. This examples allows running the solution only from local, so the docker compose file will
handle the creation of multiple containers to run the pipeline. 

```text
KAFKA_TOPIC=my-new-test-topic

POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_PASSWORD=mypassword
POSTGRES_SSLMODE=disable
POSTGRES_VERSION=13

AIRFLOW_CONN_MY_POSTGRES_DB="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"

SPARK_JDBC_URL=jdbc:postgresql://db:5432/postgres
SPARK_DB_USER=postgres
SPARK_DB_PASSWORD=mypassword
SPARK_DB_DRIVER=org.postgresql.Driver
SPARK_DB_HOST=db
SPARK_DB_PORT=5432
SPARK_DB_NAME=postgres
```

### Docker commands
As docker compose is used for a multi-container orchestration only basic commands are required to be able of running the solution.

To start all the container in daemon:

 ```docker
 docker compose up -d 
 ```
This command will start all the containers and perform the health checks required as specified in the docker-compose.yaml.
The containers handled by this command includes:
- **db**: to apply/populate the PostgreSQL schema
- **kafka**: to start the Kafka message broker used by the Pub/Sub service
- **airflow-apiserver**: to access to Airflow's UI and interact with the DAGs. 
- **grafana**: to start the visualization tool and generate the final dashboard

The volumes are created by this command as well.

```
docker compose -f docker-compose-tools.yaml up --rm consumer-mock
```

```
docker compose -f docker-compose-tools.yaml up --rm producer-mock
```
After starting the service's containers, to run the mock logic (fake data generation) the commands above are required,
these will start the consumer-mock and producer-mock services defined in the docker-compose-tools.yaml.

```
docker compose -f docker-compose-tools.yaml down
```
This command can be used to stop both the consumer-mock and producer mock

```
docker compose down
```
Finally this command will stop the service containers, to remove the volumes as well you can include the -v flag.


