import json
import random
import time

from urllib3.exceptions import ConnectTimeoutError

from src.api.api import Api


class AppApi(Api):
    def __init__(self, url, secret_key):
        super(AppApi, self).__init__(base_url=url, secret_header={'Authorization': f'Bearer {secret_key}'})

    def send_query(self, user_input, user_id: str = 'python.script', session_id: str = '', streaming_mode: bool = True,
                   retry_attempts=3, retry_backoff=1):
        headers = {'Content-Type': 'application/json'}
        payload = {
            'inputs': {},
            'query': user_input,
            'response_mode': 'streaming' if streaming_mode else 'blocking',
            'conversation_id': session_id,
            'user': user_id,
            'files': []
        }

        for attempt in range(retry_attempts):
            try:
                response = self.post('chat-messages', headers=headers, data=payload, stream=streaming_mode)
                if response.data:
                    return self._process_response_data(response.data, streaming_mode)
            except ConnectTimeoutError:
                print(f'Attempt {attempt + 1} failed due to timeout')
            if attempt < retry_attempts - 1:
                sleep_time = retry_backoff * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(sleep_time)

        raise ConnectTimeoutError(f'Failed to get API response at {self.base_url}')

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
