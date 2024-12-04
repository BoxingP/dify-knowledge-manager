from pathlib import Path
from typing import Dict, Any, Optional

from src.database.database import Database, database_session
from src.models.dify_database.datasets import Datasets
from src.models.dify_database.document_segments import DocumentSegments
from src.models.dify_database.documents import Documents
from src.models.dify_database.upload_files import UploadFiles


class DifyDatabase(Database):
    def __init__(self, database_name: str):
        super(DifyDatabase, self).__init__(database_name)

    def get_image_path(self, image_id: str) -> Optional[Path]:
        with database_session(self.session) as session:
            query = session.query(UploadFiles.key).filter(UploadFiles.id == image_id)
            result = query.first()
        return Path(result[0]) if result else None

    def get_documents(self, dataset_id: str,
                      with_segment: bool = False, is_enabled: Optional[bool] = None) -> list[Dict[str, Any]]:
        with database_session(self.session) as session:
            query = session.query(
                Documents.id.label('document_id'),
                Documents.position.label('document_position'),
                Documents.name,
                Documents.enabled,
                Datasets.id.label('dataset_id')
            ).select_from(Documents)
            query = query.outerjoin(Datasets, Datasets.id == Documents.dataset_id)
            query = query.filter(Datasets.id == dataset_id)

            if is_enabled is not None:
                query = query.filter(Documents.enabled == is_enabled)

            results = query.all()

            documents = self._process_documents(results)
            if with_segment:
                for document in documents:
                    document_id = document['id']
                    document['segment'] = self.get_segments(document_id)

            return documents

    def _process_documents(self, documents: list) -> list[Dict[str, Any]]:
        return [
            {
                'id': str(document.document_id),
                'position': document.document_position,
                'name': document.name,
                'dataset_id': str(document.dataset_id)
            } for document in documents
        ]

    def get_segments(self, document_id: str) -> list[Dict[str, Any]]:
        with database_session(self.session) as session:
            query = session.query(
                DocumentSegments.id,
                DocumentSegments.position,
                DocumentSegments.document_id,
                DocumentSegments.content,
                DocumentSegments.answer,
                DocumentSegments.keywords,
                DocumentSegments.enabled
            ).filter(
                DocumentSegments.document_id == document_id
            )
            results = query.all()
            return self._process_segments(results, query.column_descriptions)

    def _process_segments(self, segments: list, columns: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        keys = [column['name'] for column in columns]
        segments = [
            {
                key: str(value) if key in ['id', 'document_id'] else value
                for key, value in dict(zip(keys, segment)).items()
            } for segment in segments
        ]
        return segments
