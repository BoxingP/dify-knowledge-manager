from pathlib import Path

from PIL import Image
from docx import Document
from docx.image.exceptions import UnrecognizedImageError

from src.api.dify_api import DifyApi
from src.database.dify_database import DifyDatabase
from src.database.record_database import RecordDatabase
from src.services.knowledge_base import KnowledgeBase
from src.services.s3_handler import S3Handler
from src.utils.config import config


class SplitCountExceeded(Exception):
    pass


class DifyPlatform(object):
    def __init__(self, api_config: dict, record_db_name: str = 'record',
                 s3_config: dict = None, dify_db_name: str = None):
        self.api = DifyApi(api_config['api_url'], api_config['auth_token'])
        self.datasets = self.api.get_datasets()
        self.record_db = RecordDatabase(record_db_name)
        self._s3_config = s3_config
        self._s3 = None
        self._dify_db_name = dify_db_name
        self._dify_db = None

    @property
    def s3(self):
        if self._s3 is None:
            if self._s3_config is None:
                raise Exception("'s3_config' is not set, provide valid 's3_config'")
            self._s3 = S3Handler(
                self._s3_config['access_key_id'],
                self._s3_config['secret_access_key'],
                self._s3_config['region_name'],
                self._s3_config['bucket_name']
            )
        return self._s3

    @property
    def dify_db(self):
        if self._dify_db is None:
            if self._dify_db_name is None:
                raise Exception("'dify_db_name' is not set, provide valid 'dify_db_name'")
            self._dify_db = DifyDatabase(self._dify_db_name)
        return self._dify_db

    def get_dataset_id_by_name(self, name) -> str:
        dataset_id = None
        for dataset in self.datasets:
            if dataset['name'] == name:
                dataset_id = dataset['id']
                break
        return dataset_id

    def download_images_to_local(self, image_uuids: list):
        image_paths = {}
        for uuid in image_uuids:
            image_path_in_dify = self.dify_db.get_image_path(uuid)
            if self.s3.find_and_download_file(str(image_path_in_dify.as_posix()), config.image_dir_path):
                image_path = config.image_dir_path / image_path_in_dify.name
                image_paths[uuid] = image_path
        return image_paths

    def convert_image_to_jpg(self, image_path: Path) -> Path:
        jpg_image_path = config.convert_dir_path / Path(f'{image_path.stem}.jpg')
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

    def create_word_doc(self, dataset, document_id, images, split_count=1, max_split_count=3):
        if split_count > max_split_count:
            raise SplitCountExceeded(
                f'Max split count of {max_split_count} exceeded for document {document_id} in {dataset.dataset_id}')

        word_documents = []
        split_image_paths = [images[i::split_count] for i in range(split_count)]
        for i, split_images in enumerate(split_image_paths):
            file_path = config.word_dir_path / Path(f'{document_id}-{i}.docx')
            self.add_images_to_word_file(split_images, file_path)
            file_id = dataset.create_document_by_file(file_path)
            if file_id is not None:
                word_documents.append(file_id)
            else:
                word_documents.extend(
                    self.create_word_doc(dataset, document_id, split_images, split_count * 2, max_split_count))
        return word_documents

    def upload_images_to_dify(self, documents: list, dataset: KnowledgeBase) -> dict:
        images_mapping = {}

        for document in documents:
            word_docs_with_images = self.create_word_doc(dataset, document['id'], document['image'])
            new_images = []
            for document_id in word_docs_with_images:
                word_document = dataset.get_documents('api',
                                                      document_id=document_id, with_segment=True, with_image=True)
                new_images.extend(word_document['image'])
                dataset.delete_document(document_id)
            images_mapping.update(dict(zip(document['image'], new_images)))

        return images_mapping
