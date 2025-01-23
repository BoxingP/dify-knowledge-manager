from sqlalchemy import Column, String, VARCHAR, TIMESTAMP, func

from src.models.record_database.base import Base


class News(Base):
    __tablename__ = 'news'

    url = Column(String, primary_key=True)
    summary = Column(String)
    details = Column(String)
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
