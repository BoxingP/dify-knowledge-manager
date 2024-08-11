import datetime

import pandas as pd
from sqlalchemy import func, or_

from src.database.database import Database, database_session
from src.database.model import Document, Dataset, DocumentSegment


class AiDatabase(Database):
    def __init__(self):
        super(AiDatabase, self).__init__('ai')

    def save_knowledge_base_info(self, knowledge_base: dict):
        table = Dataset
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame(knowledge_base, index=[0]), table)

    def save_document(self, document: dict):
        table = Document
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame(document, index=[0]), table)

    def save_document_segment(self, segment: dict):
        table = DocumentSegment
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame.from_records([segment]), table)

    def delete_no_exist_segment(self, document_id, segment_ids):
        with database_session(self.session) as session:
            stmt = session.query(DocumentSegment) \
                .filter(DocumentSegment.document_id == document_id, ~DocumentSegment.id.in_(segment_ids))
            stmt.delete(synchronize_session='fetch')
            session.commit()

    def get_knowledge_base_documents(self, url: str, dataset_name: str) -> list:
        with database_session(self.session) as session:
            query = session.query(
                Document.id,
                Document.name,
                DocumentSegment.position,
                DocumentSegment.content,
                DocumentSegment.answer,
                DocumentSegment.keywords
            ).outerjoin(
                Document, DocumentSegment.document_id == Document.id
            ).outerjoin(
                Dataset, Dataset.id == Document.dataset_id
            ).filter(
                Dataset.url == url, Dataset.name == dataset_name
            )
            results = query.all()
        records = {}
        for result in results:
            record = records.get(result.id)
            if record is None:
                record = {"id": str(result.id), "name": result.name, "segment": []}
                records[result.id] = record
            segment = {"position": result.position, "content": result.content, "answer": result.answer,
                       "keywords": result.keywords.split(",")}
            record["segment"].append(segment)

        documents = list(records.values())
        return documents
