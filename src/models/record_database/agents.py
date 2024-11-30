from sqlalchemy import Column, Unicode, Text, Boolean, VARCHAR, TIMESTAMP, func

from src.models.record_database.base import Base


class Agents(Base):
    __tablename__ = 'agents'

    id = Column(Unicode(6), primary_key=True)
    name = Column(Unicode(50))
    country = Column(Unicode(20))
    category = Column(Unicode(20))
    language = Column(Unicode(10), primary_key=True)
    description = Column(Text)
    remark = Column(Text)
    is_active = Column(Boolean)
    is_remove = Column(Boolean)
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
