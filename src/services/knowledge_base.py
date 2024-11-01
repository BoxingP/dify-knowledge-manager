import re
import time

from src.api.dify_api import DifyApi
from src.database.record_database import RecordDatabase


class IndexingNotCompletedError(Exception):
    pass


class KnowledgeBase(object):
    def __init__(self, api: DifyApi, dataset_id, dataset_name, record_db: RecordDatabase):
        self.api = api
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.record_db = record_db

    def save_knowledge_base_info_to_db(self):
        knowledge_base_info = {'id': self.dataset_id, 'url': self.api.base_url, 'name': self.dataset_name}
        self.record_db.save_knowledge_base_info(knowledge_base_info)

    def get_documents(self, source, document_id=None, with_segment=False, with_image=False):
        if source == 'api':
            documents = self.api.get_documents_in_dataset(self.dataset_id)
            for document in documents:
                document['dataset_id'] = self.dataset_id
                if with_segment:
                    document['segment'] = self.api.get_segments_from_document(self.dataset_id, document['id'])
        elif source == 'db':
            documents = self.record_db.get_documents(self.api.base_url, self.dataset_id, with_segment)
        else:
            return None
        if with_image:
            pattern = r'!\[image\]\([^)]*/files/(.*?)/(?:image-preview|file-preview)\)'
            for document in documents:
                image = []
                for segment in document['segment']:
                    uuids = re.findall(pattern, segment['content'])
                    image.extend(uuids)
                document['image'] = image
        if document_id:
            for document in documents:
                if document['id'] == document_id:
                    return document
            return None
        return documents

    def get_segments(self, source, document_id):
        if source == 'api':
            return self.api.get_segments_from_document(self.dataset_id, document_id)
        elif source == 'db':
            return self.record_db.get_segments(document_id)
        else:
            return None

    def sync_documents_to_db(self, documents):
        origin_docs_in_db = self.get_documents(source='db', with_segment=True)
        docs_to_remove_in_db = [doc for doc in origin_docs_in_db if
                                doc['id'] not in [document['id'] for document in documents]]
        if docs_to_remove_in_db:
            self.record_db.remove_documents([doc['id'] for doc in docs_to_remove_in_db])
        self.record_db.save_documents([{k: v for k, v in document.items() if k != 'segment'} for document in documents])
        for document in documents:
            segment_ids = [segment['id'] for segment in document['segment']]
            origin_segment_ids = [segment['id'] for segment in self.get_segments('db', document['id'])]
            segments_to_remove_in_db = [segment_id for segment_id in origin_segment_ids if
                                        segment_id not in segment_ids]
            if segments_to_remove_in_db:
                self.record_db.remove_segments(document['id'], segments_to_remove_in_db)
            for segment in document['segment']:
                keywords = segment['keywords']
                if isinstance(keywords, list):
                    keywords.sort()
                    segment['keywords'] = ','.join(keywords)
            self.record_db.save_segments(document['segment'])

    def get_document_id_by_name(self, name, documents):
        for document in documents:
            if document['name'].strip().lower() == name.strip().lower():
                return document['id']
        return None

    def add_document(self, documents: list, replace_document=True, sort_document=False):
        if sort_document:
            documents = sorted(documents, key=lambda x: x['position'])
        exist_documents = None
        if replace_document:
            exist_documents = self.get_documents(source='api')
        for document in documents:
            if replace_document:
                exist_document_id = self.get_document_id_by_name(document['name'], exist_documents)
                if exist_document_id:
                    self.api.delete_document(self.dataset_id, exist_document_id)
            document_id, batch_id = self.api.create_document(self.dataset_id, document['name'])
            self.wait_document_embedding(batch_id, document_id)
            segments = document['segment']
            if isinstance(segments, dict):
                segments = [segments]
            elif sort_document:
                segments = sorted(document['segment'], key=lambda x: x['position'])
            for segment in segments:
                self.api.create_segment_in_document(self.dataset_id, document_id, segment)

    def wait_document_embedding(self, batch_id, document_id, status='completed', retry: int = 600):
        index = 0
        while self.api.get_document_embedding_status(self.dataset_id, batch_id, document_id) != status:
            if index == retry:
                raise IndexingNotCompletedError(
                    f'Indexing not completed after {retry} attempts for document_id: {document_id}')
            index += 1
            time.sleep(0.5)

    def create_document_by_file(self, file_path):
        response = self.api.create_document_by_file(self.dataset_id, file_path)
        if response is None:
            return None
        document_id = response['document']['id']
        batch_id = response['batch']
        self.wait_document_embedding(batch_id, document_id)
        return document_id

    def delete_document(self, document_id):
        self.api.delete_document(self.dataset_id, document_id)

    def update_segment_in_document(self, segment):
        self.api.update_segment_in_document(self.dataset_id, segment['document_id'], segment['id'], segment['content'],
                                            segment['answer'], segment['keywords'], True)
