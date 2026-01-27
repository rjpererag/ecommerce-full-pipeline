FROM postgres:13

COPY ./schema/schema.sql /docker-entrypoint-initdb.d/01_schema.sql
#COPY ./schema/db_populate.sql /docker-entrypoint-initdb.d/02_db_populate.sql