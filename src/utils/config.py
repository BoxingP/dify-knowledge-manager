import datetime
import os
from collections import namedtuple
from pathlib import Path
from urllib.parse import quote

import yaml
from dotenv import load_dotenv


class Config(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.initial_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        return cls._instance

    def __init__(self):
        load_dotenv()
        self.share_folder = {
            'path': Path(os.getenv('SHARE_FOLDER_PATH')).resolve(),
            'username': os.getenv('SHARE_FOLDER_USERNAME'),
            'password': os.getenv('SHARE_FOLDER_PASSWORD')
        }
        self.app_config = self._load_app_config()
        if self.app_config:
            self.mapping = self.app_config['sync']['mapping']
            self._create_path_attributes(self.app_config['path'])
            self.image_dir_path = self._resolve_path('image_dir')
            self.word_dir_path = self._resolve_path('word_dir')
            self.convert_dir_path = self._resolve_path('convert_dir')
            self.upload_dir_path = self._resolve_path('upload_dir')
            self.upload_file = self.upload_dir_path / Path(self.app_config['upload']['excel']['file_name'])
            self.upload_file_mark_column = self.app_config['upload']['excel']['mark_column']
            self.upload_file_keywords_column = self.app_config['upload']['excel']['keywords_column']
            self.upload_dataset = self.app_config['upload']['excel']['dataset']
            self.details_dataset = self.app_config['upload']['docx']['dataset']['details']
            self.summary_dataset = self.app_config['upload']['docx']['dataset']['summary']
            self.export = self.app_config['export']
            self.export_file_path = self.upload_dir_path / Path(
                f"{self.export['department'].strip().lower().replace(' ', '_')}_{self.export['file_name']}")
            self.erp_file = self.upload_dir_path / Path(self.app_config['erp']['file_name'])
            self.erp_dataset = self.app_config['erp']['dataset']

    def _load_app_config(self):
        project_root_dir = Path(__file__).parent.parent.parent
        app_config_file = os.path.join(project_root_dir, 'app_config.yaml')
        if os.path.exists(app_config_file):
            with open(app_config_file, 'r', encoding='UTF-8') as file:
                return yaml.safe_load(file)
        else:
            return {}

    def _create_path_attributes(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)

    def _resolve_path(self, key, create_if_not_exists=True):
        root_dir_path = getattr(self, 'root_dir').split(',')
        dir_path = getattr(self, key.lower(), 'tmp').split(',')
        absolute_path = Path(*root_dir_path, *dir_path).resolve()
        if create_if_not_exists and not os.path.exists(absolute_path):
            os.makedirs(absolute_path)
        return absolute_path

    def init_db_uri(self, database_name: str):
        name = database_name.upper()
        adapter = os.getenv(f'{name}_DB_ADAPTER')
        host = os.getenv(f'{name}_DB_HOST')
        port = os.getenv(f'{name}_DB_PORT')
        user = os.getenv(f'{name}_DB_USER')
        password = os.getenv(f'{name}_DB_PASSWORD')
        database_name = os.getenv(f'{name}_DB_NAME')
        db_uri = f'{adapter}://{user}:%s@{host}:{port}/{database_name}' % quote(password)
        return db_uri

    def api_config(self, name):
        api_config = namedtuple('api_config', ['url', 'app_token', 'dataset_token'])
        return api_config(
            os.getenv(f'{name}_api_server'.upper()),
            os.getenv(f'{name}_app_secret_key'.upper(), None),
            os.getenv(f'{name}_dataset_secret_key'.upper(), None)
        )

    def s3_config(self, name):
        s3_config = namedtuple('s3_config', ['access_key_id', 'secret_access_key', 'region', 'bucket'])
        return s3_config(
            os.getenv(f'{name}_access_key_id'.upper()),
            os.getenv(f'{name}_secret_access_key'.upper()),
            os.getenv(f'{name}_region'.upper()),
            os.getenv(f'{name}_s3_bucket'.upper())
        )


config = Config()
