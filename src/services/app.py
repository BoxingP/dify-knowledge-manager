import json
import re
from pathlib import Path


class App(object):
    def __init__(self, app_api):
        self.app_api = app_api
        self.user = 'python.script'

    def query_app(self, user_input, streaming_mode: bool = True, session_id: str = '', user: str = '',
                  files: list[Path] = None, parse_json: bool = True):
        if not user:
            user = self.user

        file_ids = []
        if files is not None:
            if len(files) > 3:
                raise ValueError('The number of files should be less than or equal to 3')

            for file_path in files:
                file_id = self.app_api.upload_file(file_path=file_path, user=user)
                file_ids.append(file_id)
        files_data = [{
            'type': 'image',
            'transfer_method': 'local_file',
            'upload_file_id': file_id
        } for file_id in file_ids] if file_ids else []
        response = self.app_api.send_query(
            user_input=user_input,
            streaming_mode=streaming_mode,
            session_id=session_id,
            user=user,
            files=files_data,
        )

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
