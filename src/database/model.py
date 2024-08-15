from sqlalchemy import Column, TIMESTAMP, func, Uuid, VARCHAR, Text, Integer
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = 'dataset'

    id = Column(Uuid, primary_key=True)
    url = Column(VARCHAR(255), primary_key=True)
    name = Column(VARCHAR(255))
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))


class Document(Base):
    __tablename__ = 'documents'

    id = Column(Uuid, primary_key=True)
    dataset_id = Column(Uuid, primary_key=True)
    position = Column(Integer)
    name = Column(VARCHAR(255))
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))


class DocumentSegment(Base):
    __tablename__ = 'document_segment'

    id = Column(Uuid, primary_key=True)
    document_id = Column(Uuid, primary_key=True)
    position = Column(Integer)
    content = Column(Text)
    answer = Column(Text)
    keywords = Column(Text)
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))


class UploadFiles(Base):
    __tablename__ = 'upload_files'

    id = Column(Uuid, primary_key=True)
    key = Column(VARCHAR(255))
