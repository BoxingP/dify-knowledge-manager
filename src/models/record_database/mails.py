from sqlalchemy import Column, Uuid, text, String, VARCHAR, TIMESTAMP, func, JSON

from src.models.record_database.base import Base


class Mails(Base):
    __tablename__ = 'mails'

    id = Column(Uuid, primary_key=True, server_default=text("uuid_generate_v4()"))
    entry_id = Column(String)
    message_id = Column(String)
    category = Column(String)
    sender_email = Column(String)
    sender_name = Column(String)
    cc = Column(String)
    subject = Column(String)
    sent_on = Column(String)
    received_on = Column(String)
    body = Column(String)
    html_body = Column(String)
    cleaned_body = Column(JSON)
    created_by = Column(VARCHAR(255))
    created_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
    updated_by = Column(VARCHAR(255))
    updated_on = Column(TIMESTAMP(timezone=False), server_default=func.timezone('Asia/Shanghai', func.now()))
