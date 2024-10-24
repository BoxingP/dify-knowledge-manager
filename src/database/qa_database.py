import pandas as pd

from src.database.database import Database, database_session
from src.database.model import QaKnowledge


class QaDatabase(Database):
    def __init__(self, database_name: str):
        super(QaDatabase, self).__init__(database_name)

    def get_qa_info(self, department: str):
        with database_session(self.session) as session:
            query = session.query(
                QaKnowledge.question,
                QaKnowledge.answer,
                QaKnowledge.context.label('keywords')
            ).filter(QaKnowledge.department_id == department, QaKnowledge.active.is_(True))
            df = pd.DataFrame.from_records(
                query.all(),
                columns=[column['name'] for column in query.column_descriptions]
            )
        return df
