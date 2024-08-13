import requests


class Api(object):
    def __init__(self, base_url, secret_header=None):
        self.base_url = base_url
        self.secret_header = secret_header

    def request(self, method, endpoint, **kwargs):
        url = f'{self.base_url}/{endpoint}'
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f'An HTTP error occurred: {e}')
            return None
        return response.json()

    def _merge_headers(self, headers: dict):
        return {**self.secret_header, **(headers or {})}

    def fetch_data(self, endpoint, headers=None, params=None):
        headers = self._merge_headers(headers)
        return self.request('GET', endpoint, params=params, headers=headers)

    def post_data(self, endpoint, headers=None, data=None):
        headers = self._merge_headers(headers)
        return self.request('POST', endpoint, json=data, headers=headers)

    def put_data(self, endpoint, headers=None, data=None):
        headers = self._merge_headers(headers)
        return self.request('PUT', endpoint, json=data, headers=headers)

    def delete_data(self, endpoint, headers=None):
        headers = self._merge_headers(headers)
        return self.request('DELETE', endpoint, headers=headers)
