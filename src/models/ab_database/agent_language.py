from sqlalchemy import Column, Integer, NVARCHAR

from src.models.ab_database.base import Base


class AgentLanguage(Base):
    __tablename__ = 'agent_language'

    abid = Column(Integer, primary_key=True)
    lang_code = Column(NVARCHAR(5), primary_key=True)
    name = Column(NVARCHAR(50))
    description = Column(NVARCHAR(250))
