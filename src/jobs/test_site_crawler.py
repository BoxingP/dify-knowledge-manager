import os

import pandas as pd
import pytest

from src.database.record_database import RecordDatabase
from src.pages.news_page import NewsPage


@pytest.mark.usefixtures('setup')
class TestSiteCrawler(object):
    def test_extract_news(self):
        news_page = NewsPage(self.driver)
        url = os.environ.get('URL_TO_SCRAPE')
        summary, details = news_page.extract_news(url)
        record_db = RecordDatabase('record')
        record_db.save_news(
            pd.DataFrame({
                'url': [url],
                'summary': [summary],
                'details': [details]
            })
        )
        assert True
