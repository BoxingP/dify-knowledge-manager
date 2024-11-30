from sqlalchemy import Column, Integer, NVARCHAR
from sqlalchemy.dialects.mssql import BIT

from src.models.ab_database.base import Base


class Agent(Base):
    __tablename__ = 'agent'

    abid = Column(Integer, primary_key=True)
    name = Column(NVARCHAR(50))
    description = Column(NVARCHAR(150))
    is_active = Column(BIT)
    category_id = Column(Integer)
    country_code = Column(NVARCHAR(2))
