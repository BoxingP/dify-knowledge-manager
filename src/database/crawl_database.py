import pandas as pd
from sqlalchemy import case, cast, func, Integer

from src.database.database import Database, database_session
from src.database.model import RpaCrudPInstrument


class CrawlDatabase(Database):
    def __init__(self, database_name: str):
        super(CrawlDatabase, self).__init__(database_name)

    def get_documents(self, min_news_date: int = None):
        with (database_session(self.session) as session):
            news_date_expr = cast(
                func.substring(
                    RpaCrudPInstrument.url,
                    func.charindex('/news/', RpaCrudPInstrument.url) + 6,
                    8
                ), Integer
            ).label('news_date')

            query = session.query(
                case(
                    (
                        func.charindex('\\', func.reverse(RpaCrudPInstrument.doc_path)) > 0,
                        func.right(
                            RpaCrudPInstrument.doc_path,
                            func.charindex('\\', func.reverse(RpaCrudPInstrument.doc_path)) - 1
                        )
                    ),
                    else_=RpaCrudPInstrument.doc_path
                ).label('doc_name'),
                news_date_expr
            )
            if min_news_date is not None:
                query = query.filter(news_date_expr >= min_news_date)
            query = query.order_by(
                news_date_expr.desc(),
                cast(
                    func.substring(
                        RpaCrudPInstrument.url,
                        func.charindex('/news/', RpaCrudPInstrument.url) + 15,
                        func.charindex(
                            '.',
                            func.substring(
                                RpaCrudPInstrument.url,
                                func.charindex('/news/', RpaCrudPInstrument.url) + 15,
                                func.length(RpaCrudPInstrument.url)
                            )
                        ) - 1
                    ), Integer
                ).desc()
            )
            result = session.execute(query).fetchall()

            df = pd.DataFrame.from_records(
                result,
                columns=[column['name'] for column in query.column_descriptions]
            )
            return df
