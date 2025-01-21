import json
import re

from src.api.app_api import AppApi


class Studio(object):
    def __init__(self, app_pai: AppApi):
        self.app_pai = app_pai

    def query_app(self, user_input, parse_json: bool = True, streaming_mode: bool = True):
        response = self.app_pai.send_query(user_input=user_input, streaming_mode=streaming_mode)

        try:
            if not isinstance(response, dict):
                print(f'TypeError: response is not a dictionary: {type(response)}')
                return None
            elif 'answer' not in response:
                print(f'KeyError: response does not contain an answer')
                return None
            answer = response.get('answer', '')

            if parse_json and answer:
                return self._attempt_json_parse(answer)
            return answer
        except Exception as e:
            print(f'Error: {e}')
            return None

    def _attempt_json_parse(self, answer):
        try:
            return json.loads(self._sanitize_json_response(answer))
        except json.JSONDecodeError:
            return {}

    def _sanitize_json_response(self, raw_json_str):
        raw_json_str = re.sub(r'^[^{]*', '', raw_json_str)
        raw_json_str = re.sub(r'\s*[^}\n]*$', '', raw_json_str)
        last_quote_index = raw_json_str.rfind('"')
        last_right_square_index = raw_json_str.rfind(']')
        last_brace_index = raw_json_str.rfind('}')
        if (last_quote_index > last_right_square_index
                and re.search(r'[^\s\n]', raw_json_str[last_quote_index + 1:last_brace_index])):
            raw_json_str = raw_json_str[:last_brace_index] + '"' + raw_json_str[last_brace_index:]
        return raw_json_str
