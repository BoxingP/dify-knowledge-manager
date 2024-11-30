from sqlalchemy import Column, Integer, NVARCHAR, VARCHAR

from src.models.crawl_database.base import Base


class RpaCrudPInstrument(Base):
    __tablename__ = 'rpa_crud_p_instrument'

    id = Column(Integer, primary_key=True)
    url = Column(NVARCHAR(1255))
    doc_path = Column(VARCHAR(255))
