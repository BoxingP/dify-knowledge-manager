import json
import re

from src.api.app_api import AppApi


class Studio(object):
    def __init__(self, app_pai: AppApi):
        self.app_pai = app_pai

    def query(self, query, load_json=True):
        response = self.app_pai.query_app(query=query)
        answer = response.get('answer', '')

        if load_json and answer:
            try:
                answer = json.loads(self._fix_json_str(answer))
            except json.JSONDecodeError:
                answer = {}
        return answer

    def _fix_json_str(self, json_str):
        json_str = re.sub(r'^[^{]*', '', json_str)
        json_str = re.sub(r'\s*[^}\n]*$', '', json_str)
        last_quote_index = json_str.rfind('"')
        last_right_square_index = json_str.rfind(']')
        last_brace_index = json_str.rfind('}')
        if (last_quote_index > last_right_square_index
                and re.search(r'[^\s\n]', json_str[last_quote_index + 1:last_brace_index])):
            json_str = json_str[:last_brace_index] + '"' + json_str[last_brace_index:]
        return json_str
