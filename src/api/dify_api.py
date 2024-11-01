import time

from urllib3.exceptions import ConnectTimeoutError

from src.api.api import Api


class DifyApi(Api):
    def __init__(self, url, secret_key):
        super(DifyApi, self).__init__(base_url=url, secret_header={'Authorization': f'Bearer {secret_key}'})

    def get_datasets(self, limit=20, max_retry=3, backoff_factor=1):
        page = 1
        datasets = []

        while True:
            response = None
            for _ in range(max_retry):
                response = self.fetch_data('datasets', params={'page': page, 'limit': limit})
                if response is not None:
                    break
                sleep_time = backoff_factor * (2 ** _)
                time.sleep(sleep_time)

            if response is None:
                raise ConnectTimeoutError(f'Failed to get datasets in {self.base_url}')

            datasets.extend(response['data'])
            if not response.get('has_more', False):
                break
            page += 1

        return datasets

    def get_documents_in_dataset(self, dataset_id, limit=20):
        page = 1
        has_more = True
        documents = []

        while has_more:
            endpoint = f'datasets/{dataset_id}/documents'
            params = {'page': page, 'limit': limit}
            response = self.fetch_data(endpoint, params=params)
            keys = ['id', 'position', 'name']
            documents.extend([{key: item[key] for key in keys} for item in response['data']])
            has_more = response['has_more']
            page += 1

        return documents

    def create_document(self, dataset_id, document_name, max_retry=3, backoff_factor=1):
        headers = {'Content-Type': 'application/json'}
        data = {
            'name': document_name,
            'text': '',
            'indexing_technique': 'high_quality',
            'process_rule': {
                'mode': 'automatic'
            }
        }
        for retry in range(max_retry):
            response = self.post_data(f'datasets/{dataset_id}/document/create_by_text', headers=headers, data=data)
            if response is not None:
                try:
                    return response['document']['id'], response['batch']
                except KeyError:
                    raise ValueError(
                        f'Unexpected response when creating document with name {document_name}: {response}')
            else:
                if retry < max_retry - 1:
                    sleep_time = backoff_factor * (2 ** retry)
                    time.sleep(sleep_time)
        raise ConnectTimeoutError(f'Failed to create document with name {document_name}')

    def delete_document(self, dataset_id, document_id):
        try:
            response = self.delete_data(f'datasets/{dataset_id}/documents/{document_id}')
            return f'{document_id} is deleted: {response["result"]}'
        except TypeError as e:
            return f'{document_id} has failed to delete: {e}'

    def get_segments_from_document(self, dataset_id, document_id):
        headers = {'Content-Type': 'application/json'}
        endpoint = f'datasets/{dataset_id}/documents/{document_id}/segments'
        response = self.fetch_data(endpoint, headers=headers)
        segments = response['data']
        keys = ['id', 'position', 'document_id', 'content', 'answer', 'keywords']
        segments_list = [{key: segment[key] for key in keys} for segment in segments]
        return sorted(segments_list, key=lambda segment: segment['position'])

    def create_segment_in_document(self, dataset_id, document_id, segment: dict):
        headers = {'Content-Type': 'application/json'}
        endpoint = f'datasets/{dataset_id}/documents/{document_id}/segments'
        data = {
            'segments': [
                {'content': segment.get('content'),
                 'answer': segment.get('answer', ''),
                 'keywords': segment.get('keywords', [])
                 }
            ]
        }
        response = self.post_data(endpoint, headers=headers, data=data)
        return response

    def create_document_by_file(self, dataset_id, file_path, separator='\n', max_tokens=1000):
        endpoint = f'datasets/{dataset_id}/document/create_by_file'
        data = {
            'indexing_technique': 'high_quality',
            'process_rule': {
                'rules': {
                    'pre_processing_rules': [
                        {'id': 'remove_extra_spaces', 'enabled': True}, {'id': 'remove_urls_emails', 'enabled': False}
                    ],
                    'segmentation': {'separator': separator, 'max_tokens': max_tokens}
                },
                'mode': 'custom'
            }
        }
        response = self.post_data(endpoint, data=data, file_path=file_path)
        return response

    def update_segment_in_document(self, dataset_id, document_id, segment_id, content,
                                   answer=None, keywords: list = None, enabled=None):
        headers = {'Content-Type': 'application/json'}
        endpoint = f'datasets/{dataset_id}/documents/{document_id}/segments/{segment_id}'
        data = {
            'segment': {'content': content}
        }
        if answer is not None:
            data['segment']['answer'] = answer
        if keywords is not None:
            data['segment']['keywords'] = keywords
        if enabled is not None:
            data['segment']['enabled'] = enabled
        response = self.post_data(endpoint, headers=headers, data=data)
        return response

    def get_document_embedding_status(self, dataset_id, batch_id, document_id):
        endpoint = f'datasets/{dataset_id}/documents/{batch_id}/indexing-status'
        response = self.fetch_data(endpoint)
        if response is not None and 'data' in response:
            for item in response['data']:
                if 'id' in item and item['id'] == document_id:
                    if 'indexing_status' in item:
                        return item['indexing_status']
        return ''
