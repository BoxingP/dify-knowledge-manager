from sqlalchemy import Column, Uuid, Integer, VARCHAR, TIMESTAMP, func

from src.models.record_database.base import Base


class Documents(Base):
    __tablename__ = 'documents'

    id = Column(Uuid, primary_key=True)
    dataset_id = Column(Uuid, primary_key=True)
    position = Column(Integer)
    name = Column(VARCHAR(255))
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
