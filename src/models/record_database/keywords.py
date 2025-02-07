from sqlalchemy import Column, JSON, TIMESTAMP, Uuid, VARCHAR, func, text, UniqueConstraint

from src.models.record_database.base import Base


class Keywords(Base):
    __tablename__ = 'keywords'

    id = Column(Uuid, server_default=text('uuid_generate_v4()'), unique=True, nullable=False)
    hash_value = Column(VARCHAR(255), primary_key=True, nullable=False)
    algorithm = Column(VARCHAR(64), primary_key=True, nullable=False)
    keywords = Column(JSON)
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
