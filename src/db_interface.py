
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from sqlalchemy.dialects.postgresql import insert

DEFAULT_ENGINE_PARAMS = {'pool_size': 5, 'pool_recycle': 3600}


def get_pg_db_engine(db_params=None, db_uri=None, engine_params=DEFAULT_ENGINE_PARAMS):
    """
    Create and return a SQLAlchemy engine for connecting to a PostgreSQL database.

    Parameters:
    - db_params (dict): A dictionary containing the parameters required to create the database URL. The dictionary should include all fields
        necessary to create the URL, including a database name, username, and password.

    - engine_params (dict, optional): A dictionary containing additional parameters to be passed to the SQLAlchemy create_engine function. The default value is DEFAULT_ENGINE_PARAMS, which is a dictionary with the following keys:
        - 'pool_size' (int): The maximum number of connections to keep in the connection pool. Default is 5.
        - 'pool_recycle' (int): The number of seconds after which a connection is recycled. Default is 3600.

    Returns:
    - engine (sqlalchemy.engine.Engine): A SQLAlchemy engine object that can be used to connect to the PostgreSQL database.
    """
    if not db_params:
        db_params = {}
        db_url = db_uri
    else:
        url_params = dict(
            {"drivername": "postgresql+psycopg2"}, **db_params)

        db_url = URL.create(**url_params)
    return create_engine(db_url, **engine_params)


def read_sql_from_file(path):
    """
    Reads SQL statements from a file and returns a list of non-empty statements.

    Parameters:
    - path (str): The path to the SQL file.

    Returns:
    - List[str]: A list of non-empty SQL statements.

    This function reads the contents of the specified file and splits it into individual statements based on the semicolon (;) delimiter.
    It removes any empty statements or comments starting with '--'. The resulting list contains only the non-empty SQL statements from the file.
    """
    with open(path, 'r') as reader:
        text = reader.read().strip()
        non_nulls = [statement for statement in [line.strip() for line in text.split(
            ';')] if statement != '' and statement[:2] != '--']
        return non_nulls


def execute_pg_file(path, conn):
    """
    Execute a PostgreSQL script file.

    Parameters:
    - path (str): The path to the PostgreSQL script file.
    - conn (sqlalchemy.engine.Connection): The database connection object.

    Returns:
    None

    This function reads the contents of the specified file and executes the SQL statements within it using the provided database connection. It then commits the changes made to the database.

    Note: This function assumes that the SQL statements in the file are compatible with the PostgreSQL database.
    """
    for statement in read_sql_from_file(path):
        conn.execute(text(statement))
    conn.commit()


def insert_with_duplicates(table, conn, keys, data_iterator, skip_on_conflict=True):
    """
    Inserts data into a table with the option to handle duplicate primary keys.

    Parameters:
    - table: The table object representing the table to insert data into.
    - conn: The database connection object.
    - keys: The list of column names for the data.
    - data_iterator: An iterator that yields rows of data.
    - skip_on_conflict: A boolean indicating whether to skip insertion on conflict (default: True).

    Returns:
    None

    Note:
    - If skip_on_conflict is True, the function will skip insertion of rows with duplicate primary keys.
    - If skip_on_conflict is False, the function will update existing rows with duplicate primary keys.

    """
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
    """
    Save a Socrata dataset to a database table.

    Parameters:
    - data: The dataset to save, provided as a list of dictionaries.
    - db: The SQLAlchemy database engine object.
    - table_name: The name of the table to save the dataset to.
    - schema_name: The name of the schema where the table is located.
    - data_transform_function: (optional) A function to transform the dataset (a Pandas DataFrame) before saving.
    - geometry_col: (optional) The name of the column containing geometry data.
    - **kwargs: Additional keyword arguments to pass to the pandas DataFrame.to_sql method.

    Returns:
    None

    Note:
    - The function reads the dataset chunk by chunk and appends each chunk to the same table if it already exists.
    - If a geometry column is specified, null values are replaced with null geometry.
    - If a data transformation function is specified, it is applied to the dataset before saving.
    - The function uses the insert_with_duplicates function to handle duplicate primary keys during insertion.

    """
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
