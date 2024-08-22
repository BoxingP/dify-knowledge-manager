import copy
import re
import time
from pathlib import Path

from PIL import Image
from docx import Document
from docx.image.exceptions import UnrecognizedImageError

from src.api.dify_api import DifyApi
from src.database.ai_database import AiDatabase
from src.database.dify_database import DifyDatabase


class IndexingNotCompletedError(Exception):
    pass


class KnowledgeBase(object):
    def __init__(self, api: DifyApi, knowledge_base_name):
        self.api = api
        self.dataset_id = self.api.get_dataset_id_by_name(knowledge_base_name)
        self.dataset_name = knowledge_base_name
        self.db = AiDatabase()
        self.dify_db = DifyDatabase()

    def save_knowledge_base_info_to_db(self):
        knowledge_base_info = {'id': self.dataset_id, 'url': self.api.base_url, 'name': self.dataset_name}
        self.db.save_knowledge_base_info(knowledge_base_info)

    def get_documents_segments_from_api(self, document_id=None, sync_to_database=False) -> list:
        documents = self.api.get_documents_in_dataset(self.dataset_id)
        for document in documents:
            document['dataset_id'] = self.dataset_id
            document['segment'] = self.api.get_segments_from_document(self.dataset_id, document['id'])
        if sync_to_database:
            documents_in_api = copy.deepcopy(documents)
            documents_in_db = self.db.get_documents(self.dataset_id)
            docs_to_remove_in_db = [doc for doc in documents_in_db if
                                    doc['id'] not in [document['id'] for document in documents_in_api]]
            if docs_to_remove_in_db:
                self.db.remove_documents([doc['id'] for doc in docs_to_remove_in_db])
            self.db.save_documents(
                [{k: v for k, v in document.items() if k != 'segment'} for document in documents_in_api])
            for document in documents_in_api:
                for segment in document['segment']:
                    keywords = segment['keywords']
                    if isinstance(keywords, list):
                        keywords.sort()
                        segment['keywords'] = ','.join(keywords)
                self.db.save_segments(document['segment'])
        if document_id:
            for document in documents:
                if document['id'] == document_id:
                    return [document]
        return documents

    def get_documents_segments_from_db(self) -> list:
        return self.db.get_documents_segments(self.api.base_url, self.dataset_id)

    def get_document_id_by_name(self, name, documents):
        for document in documents:
            if document['name'].strip().lower() == name.strip().lower():
                return document['id']
        return None

    def add_document(self, documents: list, replace_document=True):
        sorted_documents = sorted(documents, key=lambda x: x['position'])
        exist_documents = self.api.get_documents_in_dataset(self.dataset_id)
        for document in sorted_documents:
            if replace_document:
                exist_document_id = self.get_document_id_by_name(document['name'], exist_documents)
                if exist_document_id:
                    self.api.delete_document(self.dataset_id, exist_document_id)
            document_id = self.api.create_document(self.dataset_id, document['name'])
            sorted_segments = sorted(document['segment'], key=lambda x: x['position'])
            for segment in sorted_segments:
                self.api.create_segment_in_document(
                    self.dataset_id, document_id, segment['content'], segment['answer'], segment['keywords'])

    def get_images_from_documents(self, documents) -> list:
        pattern = r'!\[image\]\([^)]*/files/(.*?)/image-preview\)'
        documents_with_images = []
        for document in documents:
            images = []
            for segment in document['segment']:
                uuids = re.findall(pattern, segment['content'])
                images.extend(uuids)
            if images:
                record = {
                    'dataset_id': document['dataset_id'],
                    'document_id': document['id'],
                    'images': images
                }
                documents_with_images.append(record)
        return documents_with_images

    def get_image_path(self, uuid):
        return self.dify_db.get_image_path(uuid)

    def create_document_by_file(self, file_path):
        response = self.api.create_document_by_file(self.dataset_id, file_path)
        document_id = response['document']['id']
        batch_id = response['batch']
        limit = 0
        while self.api.get_document_embedding_status(self.dataset_id, batch_id, document_id) != 'completed':
            if limit == 6:
                raise IndexingNotCompletedError(
                    f'Indexing not completed after {limit} attempts for document_id: {document_id}')
            limit += 1
            time.sleep(5)
        return document_id

    def get_segments_from_document(self, document_id):
        return self.api.get_segments_from_document(self.dataset_id, document_id)

    def delete_document(self, document_id):
        self.api.delete_document(self.dataset_id, document_id)

    def update_segment_in_document(self, segment):
        self.api.update_segment_in_document(self.dataset_id, segment['document_id'], segment['id'], segment['content'],
                                            segment['answer'], segment['keywords'], True)

    def convert_image_to_jpg(self, image_path: Path) -> Path:
        jpg_image_path = f'{image_path.parent.parent / Path("converted") / image_path.stem}.jpg'
        Image.open(image_path).convert('RGB').save(jpg_image_path)
        return Path(jpg_image_path)

    def add_images_to_word_file(self, file_path: Path, images: list):
        doc = Document()
        for image in images:
            try:
                doc.add_picture(image.as_posix())
            except UnrecognizedImageError:
                jpg_image_path = self.convert_image_to_jpg(image)
                doc.add_picture(jpg_image_path.as_posix())
            doc.add_paragraph()
        doc.save(file_path.as_posix())
