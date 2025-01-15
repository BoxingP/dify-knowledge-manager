import json
import time

from urllib3.exceptions import ConnectTimeoutError

from src.api.api import Api


class AppApi(Api):
    def __init__(self, url, secret_key):
        super(AppApi, self).__init__(base_url=url, secret_header={'Authorization': f'Bearer {secret_key}'})

    def _is_error(self, response):
        if response.get('event') == 'error' and response.get('code') == 'completion_request_error':
            message = response.get('message', '')
            error_info = json.loads(message[message.index('{'): message.rindex('}') + 1])
            if error_info.get('statusCode') == 429:
                return True
        return False

    def query_app(self, query, user='python.script', conversation_id='', stream: bool = True,
                  max_attempt=3, backoff_factor=1):
        headers = {'Content-Type': 'application/json'}
        data = {
            'inputs': {},
            'query': query,
            'response_mode': 'streaming' if stream else 'blocking',
            'conversation_id': conversation_id,
            'user': user,
            'files': []
        }

        for attempt in range(max_attempt):
            response = self.post('chat-messages', headers=headers, data=data, stream=stream)
            if response.data:
                if stream and isinstance(response.data, list):
                    for item in response.data:
                        if self._is_error(item):
                            break
                    else:
                        answer = ''.join(item['answer'] for item in response.data if 'answer' in item)
                        combined_message = next(
                            (item for item in response.data if item.get('event', '').endswith('message')), None)
                        if not combined_message:
                            raise ValueError("No JSON object with 'event' field ending with 'message' found")
                        message_end = next(
                            (item for item in reversed(response.data) if item.get('event', '').endswith('message_end')),
                            None)
                        if not message_end:
                            raise ValueError("No JSON object with 'event' field ending with 'message_end' found")
                        combined_message['metadata'] = message_end.get('metadata', {})
                        combined_message['answer'] = answer
                        return combined_message
                elif not stream and isinstance(response.data, dict):
                    if self._is_error(response.data):
                        break
                    else:
                        return response.data
            elif attempt < max_attempt - 1:
                time.sleep(backoff_factor * (2 ** attempt))
        raise ConnectTimeoutError(f'Failed to query AI agent in {self.base_url}')
