from sqlalchemy import Column, Uuid, VARCHAR

from src.models.dify_database.base import Base


class UploadFiles(Base):
    __tablename__ = 'upload_files'

    id = Column(Uuid, primary_key=True)
    key = Column(VARCHAR(255))
