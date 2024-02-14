import sys
import os
from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow import DAG

# Establish reference to project root directory, to access src directory
PROJECT_ROOT_DIRECTORY = os.path.dirname(os.path.dirname(__file__))

# Bring src directory into view
sys.path.append(PROJECT_ROOT_DIRECTORY)

# Shield import statement from automatic enforcement of PEP8 standards
if True:
    from src.db_interface import read_sql_from_file

# Establish reference to Python environment containing Sqlalchemy 2.0+ for execution of database operations
EXTERNAL_PYTHON_PATH = os.path.join(
    PROJECT_ROOT_DIRECTORY, 'pg_venv/bin/python3')


# Establish reference to Postgis database to be used in Python operators below
db_conn = PostgresHook.get_connection("postgis")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@task.external_python(task_id="load_wifi_locations", python=EXTERNAL_PYTHON_PATH)
def load_wifi_locations(db_uri, proj_root_directory):
    """
    This function is wrapped in an external Python task and is used to load WiFi locations data into a PostgreSQL/PostGIS database.

    Parameters:
    - db_uri (str): The URI of the PostgreSQL database.
    - proj_root_directory (str): The root directory of the project.

    Returns:
    None

    Note:
    - The function imports necessary modules and functions into a fresh Python environment
    - It creates a SQLAlchemy engine for connecting to the PostgreSQL database.
    - It uses a Socrata result generator to retrieve WiFi locations data from a Socrata API resource.
    - The retrieved data is saved to the 'wifi_locations' table in the 'open_data' schema of the database.
    - A data transformation function is applied to the dataset before saving.

    """
    import sys
    sys.path.append(proj_root_directory)
    from src.db_interface import get_pg_db_engine, save_socrata_dataset
    from src.socrata_gen import socrata_result_generator
    from src.data_transforms import wifi_data_transform
    from src.constants import BASE_URL, MTA_WIFI_LOCATIONS_ID, APP_CREDENTIALS
    from sqlalchemy.types import JSON

    pg_db = get_pg_db_engine(db_uri=db_uri)

    wifi_data = socrata_result_generator(
        BASE_URL, MTA_WIFI_LOCATIONS_ID, api_credentials=APP_CREDENTIALS)

    save_socrata_dataset(wifi_data, pg_db, 'wifi_locations', 'open_data',
                         data_transform_function=wifi_data_transform, geometry_col='georeference', **{'dtype': {
                             'georeference': JSON,
                             'location': JSON},
                             'index': False})


@task.external_python(task_id="load_ridership", python=EXTERNAL_PYTHON_PATH)
def load_ridership(db_uri, proj_root_directory):
    """
    This function is wrapped in an external Python task and is used to load subway ridership data into a PostgreSQL/PostGIS database.

    Parameters:
    - db_uri (str): The URI of the PostgreSQL database.
    - proj_root_directory (str): The root directory of the project.

    Returns:
    None

    Note:
    - The function imports necessary modules and functions into a fresh Python environment.
    - It creates a SQLAlchemy engine for connecting to the PostgreSQL database.
    - It uses a Socrata result generator to retrieve subway ridership data from a Socrata API resource.
    - The retrieved data is saved to the 'subway_ridership' table in the 'open_data' schema of the database.
    - The function filters the data based on a specific date range and limits the number of pages retrieved.
    """

    import sys
    sys.path.append(proj_root_directory)
    from src.db_interface import get_pg_db_engine, save_socrata_dataset
    from src.socrata_gen import socrata_result_generator
    from src.constants import BASE_URL, SUBWAY_RIDERSHIP_ID, APP_CREDENTIALS
    from sqlalchemy.types import JSON

    pg_db = get_pg_db_engine(db_uri=db_uri)

    subway_ridership = socrata_result_generator(BASE_URL, SUBWAY_RIDERSHIP_ID, api_credentials=APP_CREDENTIALS,
                                                page_size=10000, max_pages=20, query_params={
                                                    # Filter for now, to keep dataset workable
                                                    '$where': 'transit_timestamp >= "2024-01-01T00:00:00.000"',
                                                    '$order': "transit_timestamp DESC"})

    # Read and save Ridership Data
    save_socrata_dataset(subway_ridership, pg_db,
                         'subway_ridership', 'open_data', **{'dtype': {'georeference': JSON}, 'index': False})


with DAG('MTA_Data_Task', default_args=default_args) as dag:

    # Gather all SQL statements held in flies in the sql/db_init/ directory
    init_scripts_path = os.path.join(PROJECT_ROOT_DIRECTORY, 'sql/db_init/')
    init_scripts = [init_scripts_path+file
                    for file in os.listdir(init_scripts_path)
                    if os.path.isfile(init_scripts_path+file)]

    init_statements = sum([read_sql_from_file(file)
                          for file in init_scripts], [])

    # Execute all SQL statements gathered above using a PostgresOperator
    db_init_task = PostgresOperator(
        task_id="table_init", postgres_conn_id="postgis_af", sql=init_statements)

    # Establish task to load wifi locations, supplying the appropriate connection and root dir reference
    load_wifi_task = load_wifi_locations(
        db_uri=db_conn.get_uri(), proj_root_directory=PROJECT_ROOT_DIRECTORY)

    # Establish task to load ridership data, supplying the appropriate connection and root dir reference
    load_ridership_task = load_ridership(
        db_uri=db_conn.get_uri(), proj_root_directory=PROJECT_ROOT_DIRECTORY)

    # Define path to file holding SQL to create materialized view for aggregates, and execute with a PostgresOperator
    materialized_view_path = os.path.join(
        PROJECT_ROOT_DIRECTORY, 'sql/agg_view.sql')
    make_materialized_view_task = PostgresOperator(task_id="make_materialized_view",
                                                   postgres_conn_id="postgis_af",
                                                   sql=read_sql_from_file(materialized_view_path))

    # Define dependencies: data loading tasks run in tandem, are preceded by db_init,
    # and are succeeded by the definition of a materialized view
    db_init_task >> load_wifi_task
    db_init_task >> load_ridership_task
    load_wifi_task >> make_materialized_view_task
    load_ridership_task >> make_materialized_view_task
