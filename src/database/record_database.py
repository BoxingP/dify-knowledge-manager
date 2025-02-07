import datetime
import uuid

import pandas as pd
import psycopg2
from dateutil.relativedelta import relativedelta
from sqlalchemy import update, func, or_, desc, asc
from sqlalchemy.exc import ProgrammingError

from src.database.database import Database, database_session
from src.models.record_database.agents import Agents
from src.models.record_database.datasets import Datasets
from src.models.record_database.document_backups import DocumentBackups
from src.models.record_database.document_segments import DocumentSegments
from src.models.record_database.documents import Documents
from src.models.record_database.docx_files import DocxFiles
from src.models.record_database.keywords import Keywords
from src.models.record_database.mails import Mails
from src.models.record_database.mails_documents_mapping import MailsDocumentsMapping
from src.models.record_database.news import News
from src.utils.config import config
from src.utils.random_generator import random_name


class RecordDatabase(Database):
    def __init__(self, database_name: str):
        super(RecordDatabase, self).__init__(database_name)
        self.tag = self._generate_tag()

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

    def get_mail_id_by_entry_id(self, entry_id):
        with database_session(self.session) as session:
            try:
                result = session.query(Mails.id).filter(func.lower(Mails.entry_id) == entry_id.lower()).first()
                if result is not None:
                    return str(result[0])
                else:
                    return None
            except ProgrammingError as e:
                if isinstance(e.orig, psycopg2.errors.UndefinedTable):
                    return None
                else:
                    raise

    def save_mails(self, mails, ignored_columns=None):
        table = Mails
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(mails, table, ignored_columns=ignored_columns)

    def convert_uuid_columns(self, df):
        for column in df.columns:
            if df[column].apply(type).eq(uuid.UUID).any():
                df[column] = df[column].astype(str)
        return df

    def get_mails(self, categories: list = None, get_recent_updated: bool = False, time_delta: relativedelta = None,
                  sort_order: str = None):
        with database_session(self.session) as session:
            table = Mails
            query = session.query(table)
            if categories is not None:
                query = query.filter(func.lower(table.category).in_([category.lower() for category in categories]))
            if get_recent_updated:
                now = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=8)))
                ago = now - time_delta
                query = query.filter(or_(table.created_on >= ago, table.updated_on >= ago))

            if sort_order == 'asc':
                query = query.order_by(asc(table.sent_on))
            elif sort_order == 'desc':
                query = query.order_by(desc(table.sent_on))
            results = query.all()
            df = pd.DataFrame(
                [{column: getattr(x, column) for column in table.__table__.columns.keys()} for x in results])
            df = self.convert_uuid_columns(df)
            return df

    def get_mail_related_document_ids(self, mail_id, dataset_id) -> list:
        try:
            with database_session(self.session) as session:
                query = session.query(
                    MailsDocumentsMapping.document_id
                ).outerjoin(
                    Documents, MailsDocumentsMapping.document_id == Documents.id
                ).filter(MailsDocumentsMapping.mail_id == mail_id.lower(), Documents.dataset_id == dataset_id.lower())
                results = query.all()
                return [str(result[0]) for result in results]
        except ProgrammingError as e:
            if isinstance(e.orig, psycopg2.errors.UndefinedTable):
                print(f'Error happens: {e}')
                return []
            else:
                raise

    def delete_document(self, document_id):
        with database_session(self.session) as session:
            session.query(DocumentSegments).filter(DocumentSegments.document_id == document_id).delete(
                synchronize_session='fetch')
            session.query(MailsDocumentsMapping).filter(MailsDocumentsMapping.document_id == document_id).delete(
                synchronize_session='fetch')
            session.query(Documents).filter(Documents.id == document_id).delete(synchronize_session='fetch')
            session.commit()

    def save_mail_document_mapping(self, mail_id, document_id, dataset_id):
        table = MailsDocumentsMapping
        self.create_table_if_not_exists(table)

        with database_session(self.session) as session:
            document_ids = [
                tup[0] for tup in session.query(table.document_id).filter(table.mail_id == mail_id.lower()).all()
            ]
            related_dataset_ids = []
            for doc_id in document_ids:
                document = session.query(Documents).filter(Documents.id == doc_id).first()
                if document is not None:
                    related_dataset_ids.append(str(document.dataset_id))
            if dataset_id in related_dataset_ids:
                existing_mapping = session.query(
                    table
                ).outerjoin(
                    Documents, table.document_id == Documents.id
                ).filter(
                    table.mail_id == mail_id, Documents.dataset_id == dataset_id
                ).update(
                    {table.document_id: document_id}, synchronize_session=False
                )
                if existing_mapping:
                    session.commit()
            else:
                new_mapping = table(mail_id=mail_id, document_id=document_id)
                session.add(new_mapping)
                session.commit()

    def backup_documents(self, documents, ignored_columns=None):
        documents['tag'] = self.tag
        print(f'Backing up {documents["document_name"].nunique()} documents with tag "{self.tag}" to database')
        table = DocumentBackups
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(documents, table, ignored_columns=ignored_columns)

    def _generate_tag(self):
        datetime_str = config.get_datetime(to_str=True)
        try:
            with database_session(self.session) as session:
                query = session.query(
                    func.substring(DocumentBackups.tag, 10).label('name')
                ).filter(DocumentBackups.tag.like(f'{datetime_str}%'))
                results = query.all()
            names = [result.name for result in results]
        except ProgrammingError as e:
            if f'relation "{DocumentBackups.__tablename__}" does not exist' in str(e):
                names = []
            else:
                raise e
        name = random_name()
        while name in names:
            name = random_name()
        return f'{datetime_str}.{name}'

    def save_news(self, news, ignored_columns=None):
        table = News
        self.create_table_if_not_exists(table)
        self.update_or_insert_data(news, table, ignored_columns=ignored_columns)

    def get_news(self, url):
        with database_session(self.session) as session:
            table = News
            query = session.query(table.summary, table.details).filter(table.url == url)
            result = query.first()
            if result:
                return result.summary, result.details
            else:
                return '', ''

    def save_keywords(self, hash_value: str, keywords: [str, list], algorithm: str, ignored_columns=None):
        table = Keywords
        self.create_table_if_not_exists(table)
        df = pd.DataFrame([{
            'hash_value': hash_value,
            'keywords': keywords,
            'algorithm': algorithm
        }])
        self.update_or_insert_data(df, table, ignored_columns=ignored_columns)

    def get_keywords(self, hash_value: str, algorithm: str) -> list:
        try:
            with database_session(self.session) as session:
                table = Keywords
                query = session.query(table.keywords).filter(
                    table.hash_value == hash_value, table.algorithm == algorithm
                )
                result = query.first()
                if result:
                    return result.keywords
                else:
                    return []
        except ProgrammingError as e:
            if f'relation "{table.__tablename__}" does not exist' in str(e):
                print(f'Table "{table.__tablename__}" does not exist')
                return []
            else:
                raise e
