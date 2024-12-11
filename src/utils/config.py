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
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.initial_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))
        load_dotenv()
        self._load_app_config()
        self._load_env_variables()
        self._resolve_paths()
        self._load_additional_config()

    def _load_app_config(self):
        app_config_file = self._get_project_root() / 'app_config.yaml'
        if app_config_file.exists():
            with open(app_config_file, 'r', encoding='UTF-8') as file:
                self.app_config = yaml.safe_load(file)
        else:
            self.app_config = {}

    def _get_project_root(self) -> Path:
        current_path = Path(__file__).resolve()
        for parent in current_path.parents:
            if (parent / '.git').exists():
                return parent
        raise RuntimeError('Project root not found')

    def _load_env_variables(self):
        ShareFolder = namedtuple('ShareFolder', ['path', 'username', 'password'])
        self.share_folder = ShareFolder(
            Path(os.getenv('SHARE_FOLDER_PATH')).resolve(),
            os.getenv('SHARE_FOLDER_USERNAME'),
            os.getenv('SHARE_FOLDER_PASSWORD')
        )

    def _resolve_paths(self):
        paths_config = self.app_config.get('paths', {})
        if not paths_config:
            return
        root_dir = Path(*paths_config.get('root_dir').split(',')).resolve()
        root_dir.mkdir(parents=True, exist_ok=True)
        setattr(self, 'root_dir_path', root_dir)

        sub_dirs = paths_config.get('sub_dirs')
        for key, value in sub_dirs.items():
            dir_path = root_dir / Path(*value.split(','))
            dir_path.mkdir(parents=True, exist_ok=True)
            setattr(self, f'{key}_path', dir_path)

    def _load_additional_config(self):
        upload_config = self.app_config.get('upload', {})
        excel_config = upload_config.get('excel', {})
        docx_config = upload_config.get('docx', {}).get('dataset', {})

        self.upload_file = getattr(self, 'upload_dir_path') / Path(excel_config.get('file_name', ''))
        self.upload_file_mark_column = excel_config.get('mark_column', '')
        self.upload_file_keywords_column = excel_config.get('keywords_column', '')
        self.upload_dataset = excel_config.get('dataset', '')

        self.details_dataset = docx_config.get('details', '')
        self.summary_dataset = docx_config.get('summary', '')

        export_config = self.app_config.get('export', {})
        self.department = export_config.get('department').strip()
        self.export_file_path = getattr(self, 'upload_dir_path') / Path(
            f"{self.department.lower().replace(' ', '_')}_{export_config.get('file_name')}")

        erp_config = self.app_config.get('erp', {})
        self.erp_file = getattr(self, 'upload_dir_path') / Path(erp_config.get('file_name', ''))
        self.erp_dataset = erp_config.get('dataset', '')

    def get_db_uri(self, database_name: str):
        name = database_name.upper()
        adapter = os.getenv(f'{name}_DB_ADAPTER')
        host = os.getenv(f'{name}_DB_HOST')
        port = os.getenv(f'{name}_DB_PORT')
        user = os.getenv(f'{name}_DB_USER')
        password = os.getenv(f'{name}_DB_PASSWORD')
        database_name = os.getenv(f'{name}_DB_NAME')
        return f'{adapter}://{user}:%s@{host}:{port}/{database_name}' % quote(password)

    def get_api_config(self, env, apps: list = None):
        env = env.upper()

        base_fields = ['url', 'dataset_token']
        app_fields = [f'{app}_app_token' for app in apps] if apps else []
        fields = base_fields + app_fields

        base_values = [os.getenv(f'{env}_API_SERVER'), os.getenv(f'{env}_DATASET_SECRET_KEY')]
        app_values = [os.getenv(f'{env}_{app.upper()}_APP_SECRET_KEY') for app in apps] if apps else []
        values = base_values + app_values

        if None in values:
            missing_vars = [var for var, value in zip(fields, values) if value is None]
            raise ValueError(f"Missing API environment variables: {', '.join(missing_vars)}")

        ApiConfig = namedtuple('ApiConfig', fields)
        return ApiConfig(*values)

    def get_s3_config(self, env):
        env = env.upper()
        S3Config = namedtuple('S3Config', ['access_key_id', 'secret_access_key', 'region', 'bucket'])
        access_key_id = os.getenv(f'{env}_ACCESS_KEY_ID')
        secret_access_key = os.getenv(f'{env}_SECRET_ACCESS_KEY')
        region = os.getenv(f'{env}_REGION')
        bucket = os.getenv(f'{env}_BUCKET')
        if None in (access_key_id, secret_access_key, region, bucket):
            missing_vars = [var for var, value in [
                (f'{env}_ACCESS_KEY_ID', access_key_id),
                (f'{env}_SECRET_ACCESS_KEY', secret_access_key),
                (f'{env}_REGION', region),
                (f'{env}_BUCKET', bucket)
            ] if value is None]
            raise ValueError(f"Missing S3 environment variables: {', '.join(missing_vars)}")

        return S3Config(access_key_id, secret_access_key, region, bucket)

    def get_sync_mapping(self):
        return self.app_config.get('sync', {}).get('mapping', {})

    def get_mailbox(self) -> list:
        return self.app_config.get('mailboxes', [])

    def get_dataset_by_category(self, category: str):
        for mailbox in self.get_mailbox():
            for subfolder in mailbox.get('inbox').get('subfolders'):
                if subfolder.get('category').lower() == category.lower():
                    return subfolder.get('dataset')
        return None


config = Config()
