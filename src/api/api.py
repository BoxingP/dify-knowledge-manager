import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from src.api.response import Response


class Api(object):
    def __init__(self, base_url: str, secret_header: Optional[Dict[str, Any]] = None):
        self.base_url = base_url
        self.secret_header = secret_header or {}

    def request(self, method: str, endpoint: str, **kwargs: Dict[str, Any]) -> Response:
        url = f'{self.base_url}/{endpoint}'
        max_attempt = kwargs.pop('max_attempt', 1)
        sleep_rate = kwargs.pop('sleep_rate', 1.0)

        for attempt in range(max_attempt):
            sleep_time = sleep_rate * (2 ** attempt)
            response = None

            try:
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                return self._process_response(response, kwargs)
            except requests.exceptions.HTTPError as e:
                if response:
                    print(f'HTTP error occurred with status code {response.status_code}: {e}')
                else:
                    print(f'HTTP error occurred with no response: {e}')
            except requests.exceptions.RequestException as e:
                print(f'Request exception occurred: {e}')
            finally:
                time.sleep(sleep_time)

            if attempt == max_attempt - 1:
                return Response(None, None)

    def _process_response(self, response: requests.Response, kwargs: Dict[str, Any]) -> Response:
        if kwargs.get('stream', False):
            events = [
                json.loads(match.group(1))
                for line in response.iter_lines()
                for line_str in [line.strip().decode('utf-8')]
                if line_str and (match := re.search(r'\w+: (\{.*})', line_str))
            ]
            return Response(response.status_code, events)
        return Response(response.status_code, response.json())

    def _merge_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = headers or {}
        headers.update(self.secret_header)
        return headers

    def _prepare_data(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        data = kwargs.pop('data', None)
        file_path = kwargs.pop('file_path', None)
        if data is not None:
            if file_path and isinstance(file_path, Path):
                data = {'data': (None, json.dumps(data), 'text/plain')}
                kwargs['files'] = {**data, 'file': (file_path.name, open(file_path, 'rb'))}
            else:
                kwargs['json'] = data

        return kwargs

    def get(self, endpoint: str, **kwargs) -> Response:
        kwargs.update(headers=self._merge_headers(kwargs.get('headers')))
        return self.request('GET', endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> Response:
        kwargs.update(headers=self._merge_headers(kwargs.get('headers')))
        kwargs = self._prepare_data(**kwargs)
        return self.request('POST', endpoint, **kwargs)

    def put(self, endpoint, **kwargs) -> Response:
        kwargs.update(headers=self._merge_headers(kwargs.get('headers')))
        kwargs = self._prepare_data(**kwargs)
        return self.request('PUT', endpoint, **kwargs)

    def delete(self, endpoint, **kwargs) -> Response:
        kwargs.update(headers=self._merge_headers(kwargs.get('headers')))
        return self.request('DELETE', endpoint, **kwargs)
