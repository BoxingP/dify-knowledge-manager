from sqlalchemy import Column, Uuid, Integer, Text, VARCHAR, TIMESTAMP, func, Boolean

from src.models.record_database.base import Base


class DocumentSegments(Base):
    __tablename__ = 'document_segments'

    id = Column(Uuid, primary_key=True)
    document_id = Column(Uuid, primary_key=True)
    position = Column(Integer)
    content = Column(Text)
    answer = Column(Text)
    keywords = Column(Text)
    enabled = Column(Boolean)
    status = Column(VARCHAR(255))
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
