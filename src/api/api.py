import requests


class Api(object):
    def __init__(self, base_url, headers=None):
        self.base_url = base_url
        self.headers = headers

    def request(self, method, endpoint, **kwargs):
        url = f'{self.base_url}/{endpoint}'
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f'An HTTP error occurred: {e}')
            return None
        return response.json()

    def fetch_data(self, endpoint, params=None):
        return self.request('GET', endpoint, params=params, headers=self.headers)

    def post_data(self, endpoint, data=None):
        return self.request('POST', endpoint, json=data, headers=self.headers)

    def put_data(self, endpoint, data=None):
        return self.request('PUT', endpoint, json=data, headers=self.headers)

    def delete_data(self, endpoint):
        return self.request('DELETE', endpoint, headers=self.headers)
