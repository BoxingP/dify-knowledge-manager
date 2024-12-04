from sqlalchemy import Column, Uuid, Integer, Text, Boolean
from sqlalchemy.dialects.postgresql import JSON

from src.models.dify_database.base import Base


class DocumentSegments(Base):
    __tablename__ = 'document_segments'

    id = Column(Uuid, primary_key=True)
    dataset_id = Column(Uuid)
    document_id = Column(Uuid)
    position = Column(Integer)
    content = Column(Text)
    keywords = Column(JSON)
    answer = Column(Text)
    enabled = Column(Boolean)
