from src.api.api import Api


class DifyApi(Api):
    def __init__(self, url, secret_key):
        headers = {'Authorization': f'Bearer {secret_key}'}
        super(DifyApi, self).__init__(base_url=url, headers=headers)

    def get_dataset_id_by_name(self, name) -> str:
        response = self.fetch_data('datasets', params={'page': 1, 'limit': 20})
        data = response['data']
        dataset_id = None
        for item in data:
            if item['name'] == name:
                dataset_id = item['id']
                break
        return dataset_id

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

    def create_document(self, dataset_id, document_name):
        self.headers['Content-Type'] = 'application/json'
        data = {
            'name': document_name,
            'text': '',
            'indexing_technique': 'high_quality',
            'process_rule': {
                'mode': 'automatic'
            }
        }
        response = self.post_data(f'datasets/{dataset_id}/document/create_by_text', data=data)
        return response['document']['id']

    def delete_document(self, dataset_id, document_id):
        try:
            response = self.delete_data(f'datasets/{dataset_id}/documents/{document_id}')
            return f'{document_id} is deleted: {response["result"]}'
        except TypeError as e:
            return f'{document_id} has failed to delete: {e}'

    def get_segments_from_document(self, dataset_id, document_id):
        self.headers['Content-Type'] = 'application/json'
        endpoint = f'datasets/{dataset_id}/documents/{document_id}/segments'
        response = self.fetch_data(endpoint)
        segments = response['data']
        keys = ['id', 'position', 'document_id', 'content', 'answer', 'keywords']
        return [{key: segment[key] for key in keys} for segment in segments]

    def create_segment_in_document(self, dataset_id, document_id, content, answer, keywords):
        self.headers['Content-Type'] = 'application/json'
        endpoint = f'datasets/{dataset_id}/documents/{document_id}/segments'
        data = {
            'segments': [{'content': content, 'answer': answer, 'keywords': keywords}]
        }
        response = self.post_data(endpoint, data=data)
        return response
