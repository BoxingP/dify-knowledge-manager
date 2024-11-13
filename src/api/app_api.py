import json
import time

from urllib3.exceptions import ConnectTimeoutError

from src.api.api import Api


class AppApi(Api):
    def __init__(self, url, secret_key):
        super(AppApi, self).__init__(base_url=url, secret_header={'Authorization': f'Bearer {secret_key}'})

    def query_ai_agent(self, query, user='python.script', conversation_id='', response_mode='streaming',
                       max_retry=3, backoff_factor=1):
        headers = {'Content-Type': 'application/json'}
        data = {
            'inputs': {},
            'query': query,
            'response_mode': response_mode,
            'conversation_id': conversation_id,
            'user': user,
            'files': []
        }
        for retry in range(max_retry):
            if response_mode == 'streaming':
                response = self.post_data('chat-messages', headers=headers, data=data, stream=True)
            elif response_mode == 'blocking':
                response = self.post_data('chat-messages', headers=headers, data=data)
            else:
                raise ValueError("response_mode must be either 'streaming' or 'blocking'")
            if response:
                if response_mode == 'streaming' and isinstance(response, list):
                    for item in response:
                        if item.get('event') == 'error' and item.get('code') == 'completion_request_error':
                            message = item.get('message', '')
                            error_info = json.loads(message[message.index('{'): message.rindex('}') + 1])
                            if error_info.get('statusCode') == 429:
                                break
                    else:
                        answer = ''.join(item['answer'] for item in response if 'answer' in item)
                        for item in response:
                            if item.get('event', '').endswith('message'):
                                combined_message = item.copy()
                                break
                        else:
                            raise ValueError("No JSON object with 'event' field ending with 'message' found")
                        for item in reversed(response):
                            if item.get('event', '').endswith('message_end'):
                                combined_message['metadata'] = item.get('metadata', {})
                                break
                        else:
                            raise ValueError("No JSON object with 'event' field ending with 'message_end' found")
                        combined_message['answer'] = answer
                        return combined_message
                elif response_mode == 'blocking' and isinstance(response, dict):
                    if response.get('event') == 'error' and response.get('code') == 'completion_request_error':
                        message = response.get('message', '')
                        error_info = json.loads(message[message.index('{'): message.rindex('}') + 1])
                        if error_info.get('statusCode') == 429:
                            break
                    else:
                        return response
            else:
                if retry < max_retry - 1:
                    sleep_time = backoff_factor * (2 ** retry)
                    time.sleep(sleep_time)
        raise ConnectTimeoutError(f'Failed to query AI agent in {self.base_url}')
