from sqlalchemy import Column, Uuid

from src.models.record_database.base import Base


class MailsDocumentsMapping(Base):
    __tablename__ = 'mails_documents_mapping'

    mail_id = Column(Uuid, primary_key=True)
    document_id = Column(Uuid, primary_key=True)
