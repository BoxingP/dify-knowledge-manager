import re
from types import SimpleNamespace
from typing import Optional

from src.api.app_api import AppApi
from src.api.dataset_api import DatasetApi
from src.database.dify_database import DifyDatabase
from src.database.record_database import RecordDatabase
from src.services.knowledge_base import KnowledgeBase
from src.services.s3_handler import S3Handler
from src.services.studio import Studio
from src.utils.config import config


class SplitCountExceeded(Exception):
    pass


class DifyPlatform(object):
    def __init__(self, env: str, apps: Optional[list[str]] = None, include_dataset: bool = True):
        self.env = env.upper()
        self.api_config = config.get_api_config(self.env, apps, include_dataset=include_dataset)

        self.studios = SimpleNamespace()
        if apps is not None:
            for app in apps:
                app_token = getattr(self.api_config, f'{app}_app_token')
                setattr(self.studios, app, Studio(AppApi(self.api_config.url, app_token)))
        self.datasets = []
        if include_dataset:
            self.dataset_api = DatasetApi(self.api_config.url, self.api_config.dataset_token)
            self.datasets = self.dataset_api.get_datasets(max_attempt=3)
        self.record_db = RecordDatabase('record')
        self._s3 = None
        self._dify_db = None

    @property
    def s3(self):
        if self._s3 is None:
            s3_config = config.get_s3_config(self.env)
            self._s3 = S3Handler(
                s3_config.access_key_id,
                s3_config.secret_access_key,
                s3_config.region,
                s3_config.bucket
            )
        return self._s3

    @property
    def dify_db(self):
        if self._dify_db is None:
            self._dify_db = DifyDatabase(self.env)
        return self._dify_db

    def get_dataset_id_by_name(self, name) -> str:
        dataset_id = None
        for dataset in self.datasets:
            if dataset['name'] == name:
                dataset_id = dataset['id']
                break
        return dataset_id

    def init_knowledge_base(self, dataset_name):
        dataset_id = self.get_dataset_id_by_name(dataset_name)
        return KnowledgeBase(dataset_id, dataset_name, self.dataset_api, self.dify_db, self.record_db)
