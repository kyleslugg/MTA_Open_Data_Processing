from socrata_gen import socrata_result_generator
from db_interface import save_socrata_dataset, get_pg_db_engine, execute_pg_file
from sqlalchemy.types import JSON
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

# Socrata API Endpoints via NYS Open Data
BASE_URL = "https://data.ny.gov/resource"

SUBWAY_RIDERSHIP_ID = 'wujg-7c2s'
MTA_WIFI_LOCATIONS_ID = 'pwa9-tmie'

# Default connection details for Postgres instance running on 5432
DEFAULT_DB_CREDENTIALS = {"database": "mta_data_task",
                          "username": 'postgres',
                          "password": 'postgres',
                          "host": "localhost",
                          "port": 5432}

APP_CREDENTIALS = {'username': os.environ.get(
    'API_KEY'), 'password': os.environ.get('API_SECRET')}

PROJECT_ROOT_DIRECTORY = os.path.dirname(os.path.dirname(__file__))

pg_db = get_pg_db_engine(DEFAULT_DB_CREDENTIALS)


# Run initialization scripts
with pg_db.connect() as conn:

    # Initialize DB with PostGIS and open_data schema
    execute_pg_file(os.path.join(PROJECT_ROOT_DIRECTORY,
                    'sql/db_init/db_init.sql'), conn)

    # Initialize tables as defined in table_init.sql
    execute_pg_file(os.path.join(
        PROJECT_ROOT_DIRECTORY, 'sql/db_init/table_init.sql'), conn)


# Get generators:
subway_ridership = socrata_result_generator(BASE_URL, SUBWAY_RIDERSHIP_ID, api_credentials=APP_CREDENTIALS, page_size=10000, max_pages=10, query_params={
                                            # Filter for now, to keep dataset workable
                                            '$where': 'transit_timestamp >= "2024-01-01T00:00:00.000"',
                                            '$order': "transit_timestamp DESC"})

wifi_data = socrata_result_generator(
    BASE_URL, MTA_WIFI_LOCATIONS_ID, api_credentials=APP_CREDENTIALS)


# Define data transformations

def wifi_data_transform(df: pd.DataFrame):
    # Address nulls in Station Name column
    df['station_name'].fillna(df['station_complex'], inplace=True)

    # Rename AT&T column
    df['att'] = df['at_t']
    df.drop(columns=['at_t'], inplace=True)

    # NOTE: Postgres automatically coerces Yes and No values (of any case, regardless of whitespace)
    # to booleans if requested, so no need to do so here

    return df


# Read and save Wifi Data
save_socrata_dataset(wifi_data, pg_db, 'wifi_locations', 'open_data',
                     data_transform_function=wifi_data_transform, geometry_col='georeference', **{'dtype': {
                         'georeference': JSON,
                         'location': JSON},
                         'index': False})

# Read and save Ridership Data
save_socrata_dataset(subway_ridership, pg_db,
                     'subway_ridership', 'open_data', **{'dtype': {'georeference': JSON}, 'index': False})

# Create and/or refresh materialized view to produce desired aggregates
with pg_db.connect() as conn:
    execute_pg_file(os.path.join(
        PROJECT_ROOT_DIRECTORY, 'sql/agg_view.sql'), conn=conn)
