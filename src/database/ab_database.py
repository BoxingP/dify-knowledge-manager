import pandas as pd
from sqlalchemy import literal
from sqlalchemy.orm import aliased

from src.database.database import Database, database_session
from src.models.ab_database.agent import Agent
from src.models.ab_database.agent_language import AgentLanguage
from src.models.ab_database.category import Category


class AbDatabase(Database):
    def __init__(self, database_name: str):
        super(AbDatabase, self).__init__(database_name)

    def get_agent_info(self) -> pd.DataFrame:
        with database_session(self.session) as session:
            a = aliased(Agent)
            al = aliased(AgentLanguage)
            c = aliased(Category)

            default_language_query = session.query(
                a.abid.label('id'),
                a.name,
                a.description,
                a.country_code.label('country'),
                c.category_name.label('category'),
                literal('default').label('language'),
                a.is_active
            ).outerjoin(c, c.category_id == a.category_id)

            language_specific_query = session.query(
                al.abid.label('id'),
                al.name,
                al.description,
                a.country_code.label('country'),
                c.category_name.label('category'),
                al.lang_code.label('language'),
                a.is_active
            ).outerjoin(
                a, a.abid == al.abid
            ).outerjoin(
                c, c.category_id == a.category_id)
            combined_query = default_language_query.union_all(language_specific_query).order_by('id')
            result = combined_query.all()

        df = pd.DataFrame.from_records(
            result,
            columns=[column['name'] for column in combined_query.column_descriptions]
        )
        df['id'] = df['id'].apply(lambda x: f'AB{x:04d}')
        return df
