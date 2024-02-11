import requests
# Create function to obtain generator over dataset


def socrata_result_generator(base_url, resource_id, query_params={}, page_size=1000, headers={}):

    # Endpoint URL
    url = f"{base_url}/{resource_id}.json"

    # Establish Paging
    page_offset = 0

    while True:
        response = requests.get(url, params=dict(
            query_params, **{'$limit': page_size, '$offset': page_offset, '$order': ':id'}), headers=headers)

      # Raise exception if request is unsuccessful
        if not response.ok:
            response.raise_for_status()

        if not response.json():
            break

        yield response.json()

        page_offset += page_size
        print(f"Page offset is now {page_offset}")
        # for line in response.iter_lines(decode_unicode=True):
        #     if line:
    #         yield line
