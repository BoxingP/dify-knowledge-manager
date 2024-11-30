import re
import time

from src.api.dataset_api import DatasetApi
from src.database.dify_database import DifyDatabase
from src.database.record_database import RecordDatabase


class IndexingNotCompletedError(Exception):
    pass


class KnowledgeBase(object):
    def __init__(self, dataset_id, dataset_name, api: DatasetApi,
                 db: DifyDatabase = None, record_db: RecordDatabase = None):
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.api = api
        self.db = db
        self.record_db = record_db

    def record_knowledge_base_info(self):
        if not self.record_db:
            raise Exception('Record database is not set')
        knowledge_base_info = {'id': self.dataset_id, 'url': self.api.base_url, 'name': self.dataset_name}
        self.record_db.save_knowledge_base_info(knowledge_base_info)

    def record_documents(self, documents):
        if not self.record_db:
            raise Exception('Record database is not set')
        docs_in_record = self.fetch_documents(source='record', with_segment=True)
        if docs_in_record is not None:
            docs_to_remove_in_record = [doc for doc in docs_in_record if
                                        doc['id'] not in [document['id'] for document in documents]]
            if docs_to_remove_in_record:
                self.record_db.remove_documents([doc['id'] for doc in docs_to_remove_in_record])
        self.record_db.save_documents([{k: v for k, v in document.items() if k != 'segment'} for document in documents])
        for document in documents:
            segment_ids = [segment['id'] for segment in document['segment']]
            segment_ids_in_record = [segment['id'] for segment in self._fetch_segments('record', document['id'])]
            segments_to_remove_in_record = [segment_id for segment_id in segment_ids_in_record if
                                            segment_id not in segment_ids]
            if segments_to_remove_in_record:
                self.record_db.remove_segments(document['id'], segments_to_remove_in_record)
            for segment in document['segment']:
                keywords = segment['keywords']
                if isinstance(keywords, list):
                    keywords.sort()
                    segment['keywords'] = ','.join(keywords)
            self.record_db.save_segments(document['segment'])

    def _fetch_all_documents(self, source):
        if source == 'api':
            return self.api.get_documents_in_dataset(self.dataset_id)
        elif source == 'db':
            if self.db is None:
                raise Exception('Dify database is not set')
            return self.db.get_documents(self.dataset_id)
        elif source == 'record':
            return self.record_db.get_documents(self.api.base_url, self.dataset_id)
        else:
            return None

    def _find_document_by_id(self, documents, document_id):
        for document in documents:
            if document['id'] == document_id:
                return document
        return None

    def _get_images_from_segments(self, segments):
        pattern = r'!\[image\]\([^)]*/files/(.*?)/(?:image-preview|file-preview)\)'
        images = []
        for segment in segments:
            uuids = re.findall(pattern, segment['content'])
            images.extend(uuids)
        return images

    def _fetch_segments(self, source, document_id):
        if source == 'api':
            return self.api.get_segments_from_document(self.dataset_id, document_id)
        elif source == 'db':
            if self.db is None:
                raise Exception('Dify database is not set')
            return self.db.get_segments(document_id)
        elif source == 'record':
            return self.record_db.get_segments(document_id)
        return None

    def _process_document(self, document, source, with_segment, with_image):
        document['dataset_id'] = self.dataset_id
        if with_segment:
            document['segment'] = self._fetch_segments(source, document['id'])
        if with_image and 'segment' in document:
            document['image'] = self._get_images_from_segments(document['segment'])

    def fetch_documents(self, source, document_id=None, with_segment=False, with_image=False):
        documents = self._fetch_all_documents(source)
        if documents is None or not documents:
            return None

        if document_id:
            document = self._find_document_by_id(documents, document_id)
            if document:
                self._process_document(document, source, with_segment, with_image)
                return document
            return None
        else:
            for document in documents:
                self._process_document(document, source, with_segment, with_image)
            return documents

    def _get_document_id_by_name(self, name, documents):
        if documents is not None:
            for document in documents:
                if document['name'].strip().lower() == name.strip().lower():
                    return document['id']
        return None

    def add_document(self, documents, replace_document=True, sort_document=False) -> dict:
        if isinstance(documents, dict):
            documents = [documents]
        elif not isinstance(documents, list):
            raise ValueError("The 'documents' parameter must be either a dict or a list of dicts")

        if sort_document:
            documents = sorted(documents, key=lambda x: x['position'])
        exist_documents = None
        if replace_document:
            exist_documents = self.fetch_documents(source='api')
        document_ids = {}
        for document in documents:
            if replace_document:
                exist_document_id = self._get_document_id_by_name(document['name'], exist_documents)
                if exist_document_id:
                    self.api.delete_document(self.dataset_id, exist_document_id)
            document_id, batch_id = self.api.create_document(self.dataset_id, document['name'])
            self._wait_document_embedding(batch_id, document_id)
            segments = document['segment']
            if isinstance(segments, dict):
                segments = [segments]
            elif sort_document:
                segments = sorted(document['segment'], key=lambda x: x['position'])
            for segment in segments:
                self.api.create_segment_in_document(self.dataset_id, document_id, segment)
            document_ids[document['name']] = document_id
        return document_ids

    def _wait_document_embedding(self, batch_id, document_id, status='completed', retry: int = 600):
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
        self._wait_document_embedding(batch_id, document_id)
        return document_id

    def delete_document(self, document_ids: list[str]):
        for document_id in document_ids:
            if document_id:
                self.api.delete_document(self.dataset_id, document_id)

    def update_segment_in_document(self, segment):
        self.api.update_segment_in_document(self.dataset_id, segment['document_id'], segment['id'], segment['content'],
                                            segment['answer'], segment['keywords'], True)

    def empty_dataset(self):
        documents = self.api.get_documents_in_dataset(self.dataset_id)
        for document in documents:
            self.api.delete_document(self.dataset_id, document['id'])
