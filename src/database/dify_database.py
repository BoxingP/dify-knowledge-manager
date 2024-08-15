from pathlib import Path

from src.database.database import Database, database_session
from src.database.model import UploadFiles


class DifyDatabase(Database):
    def __init__(self):
        super(DifyDatabase, self).__init__('dify')

    def get_image_path(self, image_id: str) -> Path:
        with database_session(self.session) as session:
            query = session.query(UploadFiles.key).filter(UploadFiles.id == image_id)
            result = query.first()
        return Path(result[0]) if result else None
