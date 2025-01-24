import json
from pathlib import Path

from src.api.api import Api


class AppApi(Api):
    SUPPORTED_MIME_TYPE = {
        'png': 'image/png',
        'jpeg': 'image/jpeg',
        'jpg': 'image/jpg',
        'webp': 'image/webp',
        'gif': 'image/gif'
    }

    def __init__(self, url, secret_key):
        super(AppApi, self).__init__(base_url=url, secret_header={'Authorization': f'Bearer {secret_key}'})
        self.user = 'python.script'

    def send_query(self, user_input, user: str = None, session_id: str = '', streaming_mode: bool = True,
                   files: list = None, max_attempt=3, sleep_rate=1):
        headers = {'Content-Type': 'application/json'}
        payload = {
            'inputs': {},
            'query': user_input,
            'response_mode': 'streaming' if streaming_mode else 'blocking',
            'conversation_id': session_id,
            'user': user if user is not None else self.user,
            'files': files if files is not None else []
        }

        response = self.post(
            endpoint='chat-messages',
            headers=headers,
            data=payload,
            stream=streaming_mode,
            max_attempt=max_attempt,
            sleep_rate=sleep_rate
        )
        if response.data:
            return self._process_response_data(response.data, streaming_mode)

    def _process_response_data(self, response_data, streaming_mode):
        if streaming_mode and isinstance(response_data, list):
            return self._handle_streaming_response(response_data)
        elif not streaming_mode and isinstance(response_data, dict):
            if self._is_response_error(response_data):
                return None
            return response_data
        return None

    def _handle_streaming_response(self, response_data):
        final_message = None
        answers = []
        metadata = None

        for item in response_data:
            if self._is_response_error(item):
                return None
            event = item.get('event', '')
            if event.endswith('thought'):
                continue
            if event.endswith('message'):
                if final_message is None:
                    final_message = item
                answers.append(item.get('answer', ''))
            elif event.endswith('message_end'):
                metadata = item.get('metadata', {})
            else:
                raise ValueError(f'Unexpected event type: {event}')

        filtered_answers = [answer for answer in answers if answer.strip()]
        if filtered_answers and metadata is not None:
            final_message['answer'] = ''.join(filtered_answers)
            final_message['metadata'] = metadata
            return final_message

    def _is_response_error(self, response):
        if response.get('event') == 'error' and response.get('code') == 'completion_request_error':
            message = response.get('message', '')
            try:
                if '{' in message and '}' in message:
                    error_info = json.loads(message[message.index('{'): message.rindex('}') + 1])
                    return error_info.get('statusCode') in [429]
            except (ValueError, json.JSONDecodeError) as e:
                print(f'Failed to parse error message: {e}')
        return False

    def upload_file(self, file_path: Path, user: str = None) -> str:
        file_extension = file_path.suffix.lower().lstrip('.')
        mime_type = self.SUPPORTED_MIME_TYPE.get(file_extension)
        if mime_type is None:
            supported_extensions = ', '.join(f'"{ext}"' for ext in self.SUPPORTED_MIME_TYPE.keys())
            print(supported_extensions)
            raise ValueError(
                f'Unsupported file extension: "{file_extension}", only {supported_extensions} are supported'
            )

        data = {
            'user': user if user is not None else self.user
        }
        files = {
            'file': (file_path.name, open(file_path, 'rb'), mime_type)
        }
        response = self.post(
            endpoint='files/upload', files=files, data=data
        )
        return getattr(response, 'data', {}).get('id', '')
