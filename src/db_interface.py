import psycopg2


def pg_connection(db_credentials):
    return psycopg2.connect(db_credentials)
