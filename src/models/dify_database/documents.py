from sqlalchemy import Column, Uuid, Integer, VARCHAR, Boolean

from src.models.dify_database.base import Base


class Documents(Base):
    __tablename__ = 'documents'

    id = Column(Uuid, primary_key=True)
    dataset_id = Column(Uuid)
    position = Column(Integer)
    name = Column(VARCHAR(255))
    enabled = Column(Boolean)
