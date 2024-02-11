FROM postgres:latest
ENV PG_PASSWORD 'postgres'
COPY ./sql/db_init/ /docker-entrypoint-initdb.d
