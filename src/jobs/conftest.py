import os

import pytest

from src.utils.config import config
from src.utils.driver_factory import DriverFactory


@pytest.fixture(scope='class')
def setup(request):
    driver = DriverFactory.get_driver(os.environ.get('BROWSER'), config.browser_headless_mode)
    driver.implicitly_wait(0)
    request.cls.driver = driver
    yield request.cls.driver
    request.cls.driver.quit()
