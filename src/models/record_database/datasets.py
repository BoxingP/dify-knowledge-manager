from sqlalchemy import Column, Uuid, VARCHAR, TIMESTAMP, func

from src.models.record_database.base import Base


class Datasets(Base):
    __tablename__ = 'datasets'

    id = Column(Uuid, primary_key=True)
    url = Column(VARCHAR(255), primary_key=True)
    name = Column(VARCHAR(255))
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
