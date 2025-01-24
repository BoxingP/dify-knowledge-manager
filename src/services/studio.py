import importlib
from types import SimpleNamespace
from typing import Optional, Any

from src.api.app_api import AppApi
from src.services.app_factory import AppFactory


class Studio(object):
    def __init__(self, apps: Optional[list[str]], api_config):
        self.module_name = 'src.services'
        self.apps = apps
        self.api_config = api_config
        self.app_factory = AppFactory()
        if self.apps is not None:
            self.app_apis = SimpleNamespace()
            for app in self.apps:
                app_token = getattr(self.api_config, f'{app}_app_token')
                setattr(self.app_apis, app, AppApi(self.api_config.url, app_token))
            self._register_apps()

    def get_app(self, app_name: str):
        if hasattr(self.app_apis, app_name):
            return self.app_factory.create_app(app_name, getattr(self.app_apis, app_name))
        else:
            raise ValueError(f'"{app_name}" app is not configured in the studio')

    def _register_apps(self):
        app_mapping = self._generate_app_mapping()
        for app, app_class in app_mapping.items():
            self.app_factory.register_app(app, app_class)

    def _generate_app_mapping(self) -> dict[str, Any]:
        app_mapping = {}
        for app in self.apps:
            app_class = self._get_app_class(app)
            if app_class:
                app_mapping[app] = app_class
        return app_mapping

    def _get_app_class(self, app_name: str):
        class_name = f'{app_name.capitalize()}Agent'
        module_path = f'{self.module_name}.{app_name}_agent'
        module = importlib.import_module(module_path)
        return getattr(module, class_name, None)
