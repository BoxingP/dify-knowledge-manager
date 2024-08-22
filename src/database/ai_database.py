import pandas as pd

from src.database.database import Database, database_session
from src.database.model import Document, Dataset, DocumentSegment


class AiDatabase(Database):
    def __init__(self):
        super(AiDatabase, self).__init__('ai')

    def save_knowledge_base_info(self, knowledge_base: dict):
        table = Dataset
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame.from_records([knowledge_base]), table)

    def save_documents(self, documents: list):
        table = Document
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame(documents), table)

    def remove_documents(self, document_ids: list):
        with database_session(self.session) as session:
            session.query(DocumentSegment) \
                .filter(DocumentSegment.document_id.in_(document_ids)).delete(synchronize_session='fetch')
            session.query(Document) \
                .filter(Document.id.in_(document_ids)).delete(synchronize_session='fetch')
            session.commit()

    def save_segments(self, segments: list):
        table = DocumentSegment
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame(segments), table)

    def remove_segments(self, document_id, segment_ids=None):
        with database_session(self.session) as session:
            if segment_ids is None:
                stmt = session.query(DocumentSegment) \
                    .filter(DocumentSegment.document_id == document_id)
            else:
                stmt = session.query(DocumentSegment) \
                    .filter(DocumentSegment.document_id == document_id, DocumentSegment.id.in_(segment_ids))
            stmt.delete(synchronize_session='fetch')
            session.commit()

    def get_documents_segments(self, url: str, dataset_id: str) -> list:
        with database_session(self.session) as session:
            query = session.query(
                Document.id.label('document_id'),
                Document.position.label('document_position'),
                Document.name,
                Dataset.id.label('dataset_id'),
                DocumentSegment.id.label('segment_id'),
                DocumentSegment.position,
                DocumentSegment.content,
                DocumentSegment.answer,
                DocumentSegment.keywords
            ).outerjoin(
                Document, DocumentSegment.document_id == Document.id
            ).outerjoin(
                Dataset, Dataset.id == Document.dataset_id
            ).filter(
                Dataset.url == url, Dataset.id == dataset_id
            )
            results = query.all()
        records = {}
        for result in results:
            record = records.get(result.document_id)
            if record is None:
                record = {
                    'id': str(result.document_id),
                    'position': result.document_position,
                    'name': result.name,
                    'dataset_id': str(result.dataset_id),
                    'segment': []
                }
                records[result.document_id] = record
            segment = {
                'id': str(result.segment_id),
                'position': result.position,
                'document_id': str(result.document_id),
                'content': result.content,
                'answer': result.answer,
                'keywords': result.keywords.split(',')
            }
            record['segment'].append(segment)

        return list(records.values())

    def get_documents(self, dataset_id: str):
        with database_session(self.session) as session:
            results = session.query(Document).filter(Document.dataset_id == dataset_id).all()
        dict_results = [
            {
                'id': str(result.id),
                'position': result.position,
                'name': result.name,
                'dataset_id': str(result.dataset_id)
            }
            for result in results
        ]
        return dict_results
