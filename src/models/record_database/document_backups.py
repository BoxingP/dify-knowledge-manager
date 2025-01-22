from sqlalchemy import Column, Uuid, Integer, Text, VARCHAR, TIMESTAMP, func, JSON, text

from src.models.record_database.base import Base


class DocumentBackups(Base):
    __tablename__ = 'document_backups'

    id = Column(Uuid, primary_key=True, server_default=text('uuid_generate_v4()'))
    environment = Column(VARCHAR(20))
    dataset_name = Column(VARCHAR(255))
    document_name = Column(VARCHAR(255))
    segment_position = Column(Integer)
    content = Column(Text)
    answer = Column(Text)
    keywords = Column(JSON)
    tag = Column(VARCHAR(255))
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
