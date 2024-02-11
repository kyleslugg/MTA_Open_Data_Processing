from socrata_gen import socrata_result_generator
from db_interface import pg_connection
from sqlalchemy import create_engine, Engine


# Socrata API Endpoints via NYS Open Data
BASE_URL = "https://data.ny.gov/resource"

SUBWAY_RIDERSHIP_ID = 'wujg-7c2s'
MTA_WIFI_LOCATIONS_ID = 'pwa9-tmie'

# Default connection details for Postgres instance running on 5432
DEFAULT_DB_CREDENTIALS = {"database": "open_data",
                          "user": 'postgres',
                          "password": 'postgres',
                          "host": "localhost",
                          "port": 5432}

# Establish DB Connection
pg_db: Engine = create_engine(connect_args=DEFAULT_DB_CREDENTIALS)
conn = pg_db.connect()


# Get generators:
subway_ridership = socrata_result_generator(BASE_URL, SUBWAY_RIDERSHIP_ID)
wifi_data = socrata_result_generator(BASE_URL, MTA_WIFI_LOCATIONS_ID)
