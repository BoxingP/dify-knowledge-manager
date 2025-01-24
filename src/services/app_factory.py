class AppFactory(object):
    def __init__(self):
        self._creators = {}

    def register_app(self, app_name, creator):
        self._creators[app_name] = creator

    def create_app(self, app_name, *args, **kwargs):
        creator = self._creators.get(app_name)
        if not creator:
            raise ValueError(f'"{app_name}" app is not registered')
        return creator(*args, **kwargs)
