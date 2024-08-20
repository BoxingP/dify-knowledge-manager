import os
from pathlib import Path
from urllib.parse import quote

import yaml
from dotenv import load_dotenv


class Config(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        load_dotenv()
        self.app_config = self._load_app_config()
        if self.app_config:
            self.mapping = self.app_config['mapping']
            self.mapping['secret_key'] = {"source": os.getenv('SOURCE_SECRET_KEY'),
                                          "target": os.getenv('TARGET_SECRET_KEY')}

    def _load_app_config(self):
        project_root_dir = Path(__file__).parent.parent.parent
        app_config_file = os.path.join(project_root_dir, 'app_config.yaml')
        if os.path.exists(app_config_file):
            with open(app_config_file, 'r', encoding='UTF-8') as file:
                return yaml.safe_load(file)
        else:
            return {}

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


config = Config()