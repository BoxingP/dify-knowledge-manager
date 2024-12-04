import pandas as pd
from sqlalchemy import update
from sqlalchemy.exc import ProgrammingError

from src.database.database import Database, database_session
from src.models.record_database.agents import Agents
from src.models.record_database.datasets import Datasets
from src.models.record_database.document_segments import DocumentSegments
from src.models.record_database.documents import Documents
from src.models.record_database.docx_files import DocxFiles


class RecordDatabase(Database):
    def __init__(self, database_name: str):
        super(RecordDatabase, self).__init__(database_name)

    def save_knowledge_base_info(self, knowledge_base: dict):
        table = Datasets
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame.from_records([knowledge_base]), table)

    def save_documents(self, documents: list):
        table = Documents
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame(documents), table)

    def remove_documents(self, document_ids: list):
        with database_session(self.session) as session:
            session.query(DocumentSegments) \
                .filter(DocumentSegments.document_id.in_(document_ids)).delete(synchronize_session='fetch')
            session.query(Documents) \
                .filter(Documents.id.in_(document_ids)).delete(synchronize_session='fetch')
            session.commit()

    def save_segments(self, segments: list):
        table = DocumentSegments
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(pd.DataFrame(segments), table)

    def remove_segments(self, document_id, segment_ids=None):
        with database_session(self.session) as session:
            if segment_ids is None:
                stmt = session.query(DocumentSegments) \
                    .filter(DocumentSegments.document_id == document_id)
            else:
                stmt = session.query(DocumentSegments) \
                    .filter(DocumentSegments.document_id == document_id, DocumentSegments.id.in_(segment_ids))
            stmt.delete(synchronize_session='fetch')
            session.commit()

    def get_documents(self, url: str, dataset_id: str, with_segment=False, is_enabled: bool = None) -> list:
        try:
            with database_session(self.session) as session:
                query = session.query(
                    Documents.id.label('document_id'),
                    Documents.position.label('document_position'),
                    Documents.name,
                    Datasets.id.label('dataset_id'),
                    DocumentSegments.id.label('segment_id'),
                    DocumentSegments.position,
                    DocumentSegments.content,
                    DocumentSegments.answer,
                    DocumentSegments.keywords
                ).outerjoin(
                    Documents, DocumentSegments.document_id == Documents.id
                ).outerjoin(
                    Datasets, Datasets.id == Documents.dataset_id
                ).filter(
                    Datasets.url == url, Datasets.id == dataset_id
                )
                if is_enabled is not None:
                    query = query.filter(Documents.enabled == is_enabled)
                results = query.all()
            records = {}
            for result in results:
                record = records.get(result.document_id)
                if record is None:
                    record = {
                        'id': str(result.document_id),
                        'position': result.document_position,
                        'name': result.name,
                        'dataset_id': str(result.dataset_id)
                    }
                    if with_segment:
                        record['segment'] = []
                    records[result.document_id] = record
                if with_segment:
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
        except ProgrammingError as e:
            if f'relation "{Documents.__tablename__}" does not exist' in str(e):
                print(f'Table "{Documents.__tablename__}" does not exist. Returning an empty list.')
                return []
            elif f'relation "{DocumentSegments.__tablename__}" does not exist' in str(e):
                print(f'Table "{DocumentSegments.__tablename__}" does not exist. Returning an empty list.')
                return []
            else:
                raise e

    def get_segments(self, document_id):
        with database_session(self.session) as session:
            try:
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
                keys = [column['name'] for column in query.column_descriptions]
                segments = [{**{
                    key: str(value) if key in ['id', 'document_id'] else value.split(
                        ", ") if key == 'keywords' else value
                    for key, value in dict(zip(keys, result)).items()}} for result in results]
                return segments
            except ProgrammingError as e:
                if f'relation "{DocumentSegments.__tablename__}" does not exist' in str(e):
                    print(f'Table "{DocumentSegments.__tablename__}" does not exist. Returning an empty list.')
                    return []
                else:
                    raise e

    def save_docx_file(self, docx_file: pd.DataFrame):
        table = DocxFiles
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(docx_file, table, ignored_columns=['id'])

    def get_docx_file(self):
        with database_session(self.session) as session:
            try:
                query = session.query(
                    DocxFiles.name,
                    DocxFiles.extension,
                    DocxFiles.hash
                )
                df = pd.DataFrame.from_records(
                    query.all(),
                    columns=[column['name'] for column in query.column_descriptions]
                )
                return df
            except ProgrammingError as e:
                if f'relation "{DocxFiles.__tablename__}" does not exist' in str(e):
                    print(f'Table "{DocxFiles.__tablename__}" does not exist. Returning an empty dataframe.')
                    return pd.DataFrame(columns=['name', 'extension', 'hash'])
                else:
                    raise e

    def save_agent_info(self, agent_info: pd.DataFrame):
        table = Agents
        self.create_table_if_not_exists(table)
        with database_session(self.session) as session:
            query = session.query(table)
            existing_agent_info = pd.DataFrame([record.__dict__ for record in query.all()])
            if not existing_agent_info.empty:
                existing_agent_info = existing_agent_info.drop('_sa_instance_state', axis=1)
                merged_df = existing_agent_info.merge(
                    agent_info[['id', 'language']], on=['id', 'language'], how='left', indicator=True
                )
                rows_to_remove = merged_df[merged_df['_merge'] == 'left_only']
                if not rows_to_remove.empty:
                    session.execute(
                        update(table)
                        .where(table.id.in_(rows_to_remove['id']), table.language.in_(rows_to_remove['language']))
                        .values(is_remove=True)
                    )
                    session.commit()
            agent_info['is_remove'] = False
            self.update_or_insert_data(agent_info, table)

    def get_agent_info(self) -> pd.DataFrame:
        with database_session(self.session) as session:
            query = session.query(
                Agents.id.label('abid'),
                Agents.name,
                Agents.country,
                Agents.category,
                Agents.language,
                Agents.description,
                Agents.remark
            ).filter(
                Agents.is_active == True,
                Agents.is_remove == False
            )

            df = pd.DataFrame.from_records(
                query.all(),
                columns=[column['name'] for column in query.column_descriptions]
            )
            return df
