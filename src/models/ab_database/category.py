from sqlalchemy import Column, NVARCHAR, Integer

from src.models.ab_database.base import Base


class Category(Base):
    __tablename__ = 'category'

    category_id = Column(Integer, primary_key=True)
    category_name = Column(NVARCHAR(20), primary_key=True)
