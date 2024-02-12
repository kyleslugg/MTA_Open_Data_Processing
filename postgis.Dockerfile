FROM postgis/postgis:latest
ENV POSTGRES_PASSWORD 'postgres'
ENV POSTGRES_DB 'mta_data_task'
COPY ./sql/db_init/ /docker-entrypoint-initdb.d

