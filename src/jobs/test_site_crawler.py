import os

import pytest

from src.pages.news_page import NewsPage


@pytest.mark.usefixtures('setup')
class TestSiteCrawler(object):
    def test_extract_news(self):
        news_page = NewsPage(self.driver)
        summary, details = news_page.extract_news(os.environ.get('URL_TO_SCRAPE'))
        os.environ['SUMMARY'] = summary
        os.environ['DETAILS'] = details
        assert True
