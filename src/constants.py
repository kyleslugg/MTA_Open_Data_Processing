
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ROOT_DIRECTORY = os.path.dirname(os.path.dirname(__file__))

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
