from socrata_gen import socrata_result_generator


# Socrata API Endpoints via NYS Open Data
BASE_URL = "https://data.ny.gov/resource"

SUBWAY_RIDERSHIP_ID = 'wujg-7c2s'
MTA_WIFI_LOCATIONS_ID = 'pwa9-tmie'


# Get generators:
subway_ridership = socrata_result_generator(BASE_URL, SUBWAY_RIDERSHIP_ID)
wifi_data = socrata_result_generator(BASE_URL, MTA_WIFI_LOCATIONS_ID)
