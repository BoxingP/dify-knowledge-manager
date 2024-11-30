from sqlalchemy import Column, Integer, VARCHAR, Boolean

from src.models.qa_database.base import Base


class QaKnowledge(Base):
    __tablename__ = 'qa_knowledge'

    id = Column(Integer, primary_key=True)
    question = Column(VARCHAR(1000))
    answer = Column(VARCHAR(50000))
    context = Column(VARCHAR(10000))
    active = Column(Boolean)
    department_id = Column(VARCHAR(50))
