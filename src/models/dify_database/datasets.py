from sqlalchemy import Column, Uuid, VARCHAR

from src.models.dify_database.base import Base


class Datasets(Base):
    __tablename__ = 'datasets'

    id = Column(Uuid, primary_key=True)
    name = Column(VARCHAR(255))
