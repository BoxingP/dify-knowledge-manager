import re
import time
from pathlib import Path

from PIL import Image
from docx import Document
from docx.image.exceptions import UnrecognizedImageError

from src.api.dify_api import DifyApi
from src.database.ai_database import AiDatabase
from src.database.dify_database import DifyDatabase
from src.services.s3_handler import S3Handler
from src.utils.config import config


class IndexingNotCompletedError(Exception):
    pass


class KnowledgeBase(object):
    def __init__(self, api: DifyApi, knowledge_base_name):
        self.api = api
        self.dataset_id = self.api.get_dataset_id_by_name(knowledge_base_name)
        self.dataset_name = knowledge_base_name
        self.db = AiDatabase()
        self.dify_db = DifyDatabase()
        self.assets_root_path = Path(__file__).parent.parent.absolute() / Path('assets')

    def save_knowledge_base_info_to_db(self):
        knowledge_base_info = {'id': self.dataset_id, 'url': self.api.base_url, 'name': self.dataset_name}
        self.db.save_knowledge_base_info(knowledge_base_info)

    def get_documents(self, source, document_id=None, with_segment=False, with_image=False):
        if source == 'api':
            documents = self.api.get_documents_in_dataset(self.dataset_id)
            for document in documents:
                document['dataset_id'] = self.dataset_id
                if with_segment:
                    document['segment'] = self.api.get_segments_from_document(self.dataset_id, document['id'])
        elif source == 'db':
            documents = self.db.get_documents(self.api.base_url, self.dataset_id, with_segment)
        else:
            return None
        if with_image:
            pattern = r'!\[image\]\([^)]*/files/(.*?)/image-preview\)'
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

    def sync_documents_to_db(self, documents):
        origin_docs_in_db = self.get_documents(source='db', with_segment=True)
        docs_to_remove_in_db = [doc for doc in origin_docs_in_db if
                                doc['id'] not in [document['id'] for document in documents]]
        if docs_to_remove_in_db:
            self.db.remove_documents([doc['id'] for doc in docs_to_remove_in_db])
        self.db.save_documents([{k: v for k, v in document.items() if k != 'segment'} for document in documents])
        for document in documents:
            for segment in document['segment']:
                keywords = segment['keywords']
                if isinstance(keywords, list):
                    keywords.sort()
                    segment['keywords'] = ','.join(keywords)
            self.db.save_segments(document['segment'])

    def get_document_id_by_name(self, name, documents):
        for document in documents:
            if document['name'].strip().lower() == name.strip().lower():
                return document['id']
        return None

    def add_document(self, documents: list, replace_document=True):
        sorted_documents = sorted(documents, key=lambda x: x['position'])
        exist_documents = self.get_documents(source='api')
        for document in sorted_documents:
            if replace_document:
                exist_document_id = self.get_document_id_by_name(document['name'], exist_documents)
                if exist_document_id:
                    self.api.delete_document(self.dataset_id, exist_document_id)
            document_id, batch_id = self.api.create_document(self.dataset_id, document['name'])
            self.wait_document_embedding(batch_id, document_id)
            sorted_segments = sorted(document['segment'], key=lambda x: x['position'])
            for segment in sorted_segments:
                self.api.create_segment_in_document(self.dataset_id, document_id, segment)

    def get_image_paths(self, image_uuids: list):
        image_paths = []
        s3_handler = S3Handler(config.aws['access_key_id'], config.aws['secret_access_key'],
                               config.aws['region'], config.aws['s3_bucket'])
        for uuid in image_uuids:
            image_path_in_dify = self.dify_db.get_image_path(uuid)
            if s3_handler.find_and_download_file(str(image_path_in_dify.as_posix()), config.image_location):
                image_path = config.image_location / image_path_in_dify.name
                image_paths.append(image_path)
        return image_paths

    def wait_document_embedding(self, batch_id, document_id, status='completed', retry: int = 6):
        index = 0
        while self.api.get_document_embedding_status(self.dataset_id, batch_id, document_id) != status:
            if index == retry:
                raise IndexingNotCompletedError(
                    f'Indexing not completed after {retry} attempts for document_id: {document_id}')
            index += 1
            time.sleep(5)

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

    def convert_image_to_jpg(self, image_path: Path) -> Path:
        jpg_image_path = self.assets_root_path / Path('image_files/converted') / f'{image_path.stem}.jpg'
        Image.open(image_path).convert('RGB').save(jpg_image_path)
        return Path(jpg_image_path)

    def add_images_to_word_file(self, images: list, word_file: Path):
        doc = Document()
        for image in images:
            try:
                doc.add_picture(image.as_posix())
            except UnrecognizedImageError:
                jpg_image_path = self.convert_image_to_jpg(image)
                doc.add_picture(jpg_image_path.as_posix())
            doc.add_paragraph()
        doc.save(word_file.as_posix())

    def upload_images_to_knowledge_base(self, documents: list) -> dict:
        images_mapping = {}
        for document in documents:
            image_paths = self.get_image_paths(document['image'])
            word_file_path = self.assets_root_path / Path('word_files') / Path(f"{document['id']}.docx")
            self.add_images_to_word_file(image_paths, word_file_path)
            word_file_id = self.create_document_by_file(word_file_path)
            word_documents = [word_file_id]
            if word_file_id is None:
                midpoint = len(image_paths) // 2
                first_half = image_paths[:midpoint]
                second_half = image_paths[midpoint:]
                first_file_path = self.assets_root_path / Path('word_files') / Path(f"{document['id']}-1.docx")
                second_file_path = self.assets_root_path / Path('word_files') / Path(f"{document['id']}-2.docx")
                self.add_images_to_word_file(first_half, first_file_path)
                self.add_images_to_word_file(second_half, second_file_path)
                first_file_id = self.create_document_by_file(first_file_path)
                second_file_id = self.create_document_by_file(second_file_path)
                word_documents = [first_file_id, second_file_id]
            new_images = []
            for document_id in word_documents:
                word_document = self.get_documents('api', document_id=document_id, with_segment=True, with_image=True)
                new_images.extend(word_document['image'])
                self.delete_document(document_id)
            images_mapping.update(dict(zip(document['image'], new_images)))

        return images_mapping
