import logging
import requests
from requests.auth import HTTPBasicAuth
# Create function to obtain generator over dataset


def socrata_result_generator(base_url, resource_id, api_credentials=None, query_params=None, page_size=1000, page_offset=0, max_pages=None, headers=None):
    """
    Creates a generator over a Socrata API resource in a paginated manner.

    Args:
        base_url (str): The base URL of the Socrata API.
        resource_id (str): The ID of the resource to retrieve data from.
        api_credentials (dict, optional): A dictionary containing the API credentials for basic authentication. Defaults to None.
        query_params (dict, optional): Additional query parameters to include in the API request. Defaults to None.
        page_size (int, optional): The number of results to retrieve per page. Defaults to 1000.
        page_offset (int, optional): The starting offset for pagination. Defaults to 0.
        max_pages (int, optional): The maximum number of pages to retrieve. Defaults to None, leading all pages to be retrieved.
        headers (dict, optional): Additional headers to include in the API request. Defaults to None.

    Yields:
        dict: A dictionary representing a page of results from the Socrata API.

    Raises:
        HTTPError: If the API request is unsuccessful.

    Example:
        for result in socrata_result_generator(base_url, resource_id):
            # Process the result
            pass
    """

    # Endpoint URL
    url = f"{base_url}/{resource_id}.json"

    # Choose whether to include basic authentication
    basic_auth = HTTPBasicAuth(
        **api_credentials) if api_credentials is not None else None

    # Assign empty dictionary if query_params is None
    if query_params is None:
        query_params = {}

    # Assign empty dictionary if headers is None
    if headers is None:
        headers = {}

    while max_pages is None or page_offset/page_size < max_pages:
        response = requests.get(url, params=dict(
            {'$limit': page_size, '$offset': page_offset, '$order': ':id'}, **query_params), headers=headers, auth=basic_auth)

        # Raise exception if request is unsuccessful
        if response.status_code != 200:
            response.raise_for_status()

        result = response.json()
        if not result:
            break

        yield result

        page_offset += page_size
        logging.info(f"Page offset is now {page_offset}")
        print(f"Page offset is now {page_offset}")
