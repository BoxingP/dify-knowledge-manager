from sqlalchemy import Column, Uuid, text, VARCHAR, TIMESTAMP, func

from src.models.record_database.base import Base


class DocxFiles(Base):
    __tablename__ = 'docx_files'

    id = Column(Uuid, server_default=text('uuid_generate_v4()'))
    name = Column(VARCHAR(255), primary_key=True)
    extension = Column(VARCHAR(255), primary_key=True)
    hash = Column(VARCHAR(255))
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
