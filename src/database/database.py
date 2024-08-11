import datetime
from contextlib import contextmanager

from sqlalchemy import create_engine, inspect, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from src.utils.config import config


@contextmanager
def database_session(session):
    try:
        yield session
    finally:
        session.close()


class Database(object):

    def __init__(self, database_name: str):
        self.database_name = database_name
        self.db_uri = config.init_db_uri(database_name)
        self.engine = self._create_engine()
        self.session = self._create_session()

    def _create_engine(self):
        return create_engine(self.db_uri, echo=False)

    def _create_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()

    def create_table_if_not_exists(self, table):
        inspector = inspect(self.session.bind)
        if not inspector.has_table(table.__tablename__):
            table.__table__.create(self.session.bind, checkfirst=True)

    def get_table_primary_key_column_names(self, table) -> list:
        table = Table(table.__tablename__, MetaData(), autoload_with=self.engine)
        if table.primary_key.columns.values():
            return [col.name for col in table.primary_key.columns.values()]
        else:
            return []

    def get_column_types(self, table):
        mapper = inspect(table)
        column_types = {column.key: column.type for column in mapper.columns}
        return column_types

    def update_or_insert_data(self, dataframe, table, column_mapping: dict = None, temp_table_name='temp_data_df',
                              track_change=True, ignored_columns: list = None):
        if ignored_columns is None:
            ignored_columns = []
        if column_mapping:
            dataframe.rename(columns=column_mapping, inplace=True)
        if track_change:
            dataframe['created_by'] = 'Created By Script'
            dataframe['created_on'] = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=8)))
            dataframe['updated_by'] = 'Updated By Script'
            dataframe['updated_on'] = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=8)))

        with database_session(self.session) as session:
            session.execute(text(f'DROP TABLE IF EXISTS {temp_table_name} CASCADE;'))
            session.commit()

            pk_column_names = self.get_table_primary_key_column_names(table)
            excluded_columns = pk_column_names + ['created_by', 'created_on', 'updated_by', 'updated_on']
            if ignored_columns is not None:
                excluded_columns.extend(ignored_columns)
            chk_columns = [col.name for col in table.__table__.columns if col.name not in excluded_columns]
            temp_column_types = self.get_column_types(table)
            dtype_dict = {col: temp_column_types[col] for col in temp_column_types}
            dataframe.to_sql(temp_table_name, session.bind, index=False, if_exists='replace',
                             dtype=dtype_dict)
            temp_table_class = Table(temp_table_name, MetaData(), autoload_with=session.bind)
            stmt = insert(table).from_select(temp_table_class.columns, temp_table_class.select())
            if chk_columns:
                where_condition = text('OR '.join(
                    [f'({table.__tablename__}.{col} IS DISTINCT FROM excluded.{col})' for col in chk_columns]))
            else:
                where_condition = None
            stmt = stmt.on_conflict_do_update(
                index_elements=pk_column_names,
                set_={col.name: stmt.excluded[col.name] for col in temp_table_class.columns if
                      col.name not in {'created_by', 'created_on'}},
                where=where_condition
            )
            session.execute(stmt)
            session.commit()

            session.execute(text(f'DROP TABLE IF EXISTS {temp_table_name} CASCADE;'))
            session.commit()
