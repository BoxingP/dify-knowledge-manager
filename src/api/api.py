import json
import re
from pathlib import Path

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

        if kwargs.get('stream', False):
            events = []
            for line in response.iter_lines():
                line = line.strip()
                if line:
                    line_str = line.decode('utf-8')
                    match = re.search(r'\w+: (\{.*})', line_str)
                    if match:
                        json_str = match.group(1)
                        events.append(json.loads(json_str))
                    else:
                        continue
            return events
        else:
            return response.json()

    def _merge_headers(self, headers: dict):
        return {**self.secret_header, **(headers or {})}

    def fetch_data(self, endpoint, headers=None, params=None):
        headers = self._merge_headers(headers)
        return self.request('GET', endpoint, params=params, headers=headers)

    def post_data(self, endpoint, headers=None, data=None, file_path: Path = None, **kwargs):
        headers = self._merge_headers(headers)
        if file_path:
            data = {'data': (None, json.dumps(data), 'text/plain')}
            with open(file_path, 'rb') as file:
                files = {'file': (file_path.name, file)}
                return self.request('POST', endpoint, headers=headers, files={**data, **files}, **kwargs)
        else:
            return self.request('POST', endpoint, json=data, headers=headers, **kwargs)

    def put_data(self, endpoint, headers=None, data=None):
        headers = self._merge_headers(headers)
        return self.request('PUT', endpoint, json=data, headers=headers)

    def delete_data(self, endpoint, headers=None):
        headers = self._merge_headers(headers)
        return self.request('DELETE', endpoint, headers=headers)
