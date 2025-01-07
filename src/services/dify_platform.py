import json
import re

from src.api.app_api import AppApi
from src.api.dataset_api import DatasetApi
from src.database.dify_database import DifyDatabase
from src.database.record_database import RecordDatabase
from src.services.knowledge_base import KnowledgeBase
from src.services.s3_handler import S3Handler
from src.utils.config import config


class SplitCountExceeded(Exception):
    pass


class DifyPlatform(object):
    def __init__(self, env: str, apps: list = None):
        self.env = env.upper()
        self.api_config = config.get_api_config(self.env, apps)
        if apps is not None:
            for app in apps:
                app_token = getattr(self.api_config, f'{app}_app_token')
                setattr(self, f'{app}_api', AppApi(self.api_config.url, app_token))
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

    def download_images_to_local(self, image_uuids: list, skip_if_exists=False):
        if self.dify_db is None:
            raise Exception('Dify database is not set')
        image_paths = {}
        for uuid in image_uuids:
            image_path_in_dify = self.dify_db.get_image_path(uuid)
            if image_path_in_dify is None:
                continue
            image_path = config.image_dir_path / image_path_in_dify.name
            if skip_if_exists and image_path.exists():
                print(f'skip downloading image "{image_path_in_dify.name}" as it already exists')
                image_paths[uuid] = image_path
                continue
            if self.s3.find_and_download_file(
                    str(image_path_in_dify.as_posix()), config.image_dir_path, skip_if_exists
            ):
                image_paths[uuid] = image_path
        return image_paths

    def fix_json_str(self, json_str):
        json_str = re.sub(r'^[^{]*', '', json_str)
        json_str = re.sub(r'\s*[^}\n]*$', '', json_str)
        last_quote_index = json_str.rfind('"')
        last_right_square_index = json_str.rfind(']')
        last_brace_index = json_str.rfind('}')
        if (last_quote_index > last_right_square_index
                and re.search(r'[^\s\n]', json_str[last_quote_index + 1:last_brace_index])):
            json_str = json_str[:last_brace_index] + '"' + json_str[last_brace_index:]
        return json_str

    def analyze_content(self, app_api, query: str):
        response = app_api.query_ai_agent(query, stream=True)
        try:
            answer = json.loads(self.fix_json_str(response.get('answer', {})))
        except json.JSONDecodeError:
            answer = {}
        return answer

    def init_knowledge_base(self, dataset_name):
        dataset_id = self.get_dataset_id_by_name(dataset_name)
        return KnowledgeBase(dataset_id, dataset_name, self.dataset_api, self.dify_db, self.record_db)
