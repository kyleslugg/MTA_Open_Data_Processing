
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from sqlalchemy.dialects.postgresql import insert

DEFAULT_ENGINE_PARAMS = {'pool_size': 5, 'pool_recycle': 3600}


def get_pg_db_engine(db_params, engine_params=DEFAULT_ENGINE_PARAMS):
    url_params = dict(
        {"drivername": "postgresql+psycopg2"}, **db_params)

    db_url = URL.create(**url_params)
    return create_engine(db_url, **engine_params)


def execute_pg_file(path, conn):
    with open(path, 'r') as reader:
        sql_text = reader.read()
        conn.execute(text(sql_text))
        conn.commit()


def insert_with_duplicates(table, conn, keys, data_iterator, skip_on_conflict=True):
    # Define custom insertion method to handle cases where primary keys are duplicated
    data = [dict(zip(keys, row)) for row in data_iterator]

    insert_statement = insert(table.table).values(data)

    if not skip_on_conflict:
        upsert_statement = insert_statement.on_conflict_do_update(
            constraint=f"{table.table.name}_pkey",
            set_={c.key: c for c in insert_statement.excluded},
        )
    else:
        upsert_statement = insert_statement.on_conflict_do_nothing(
            constraint=f"{table.table.name}_pkey"
        )

    conn.execute(upsert_statement)


def save_socrata_dataset(data, db, table_name, schema_name, data_transform_function=None, geometry_col=None, **kwargs):

    # Read dataset chunk by chunk, and append each new chunk to the same table if it already exists
    with db.connect() as conn:
        for chunk in data:
            df = pd.DataFrame.from_dict(chunk)

            # Screen geometry columns for null values, and replace with null geometry
            if geometry_col:
                df[geometry_col] = df[geometry_col].apply(
                    lambda x: {"type": "Point", "coordinates": []} if x == 'null' or pd.isna(x) else x)

            # Apply data transformations if specified
            if data_transform_function:
                df = data_transform_function(df)

            df.to_sql(table_name, conn, if_exists='append',
                      schema=schema_name, method=insert_with_duplicates, **kwargs)
