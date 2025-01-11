import os

import pytest

from src.utils.config import config
from src.utils.random_generator import random_browser


def scrape_web_page_content(url):
    random_browser()
    os.environ['URL_TO_SCRAPE'] = url
    result = pytest.main(
        [f"{config.jobs_dir}", '--cache-clear', '-s']
    )
    if result == 0:
        return os.environ.get('SUMMARY'), os.environ.get('DETAILS')
    else:
        print(f"Scraping {url} failed.")
        return '', ''
