from selenium.common import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from src.pages.locators import PageLocators
from src.utils.config import config


class Page(object):
    def __init__(self, driver):
        self.driver = driver
        self.timeout = config.browser_timeout
        self.locator = PageLocators

    def open_page(self, url='', wait_string_in_url: str = None, wait_element=None):
        self.driver.get(url)
        if wait_string_in_url is None:
            self._wait_string_in_url(wait_string_in_url)
        if wait_element is not None:
            self._wait_element_to_be_visible(*wait_element)

    def _find_element(self, *locator):
        return self.driver.find_element(*locator)

    def _get_url(self):
        return self.driver.current_url

    def _wait_string_in_url(self, string):
        try:
            WebDriverWait(self.driver, timeout=self.timeout).until(EC.url_contains(string))
        except TimeoutException:
            print(f'\n * url not contains {string} within {self.timeout} seconds! --> current url is {self._get_url()}')

    def _wait_element_to_be_visible(self, *locator):
        try:
            WebDriverWait(self.driver, timeout=self.timeout).until(EC.visibility_of_element_located(locator))
        except TimeoutException:
            print(f'\n * element not visible within {self.timeout} seconds! --> {locator[1]}')

    def _wait_frame_to_be_visible(self, *locator):
        try:
            WebDriverWait(self.driver, timeout=self.timeout).until(EC.frame_to_be_available_and_switch_to_it(locator))
        except TimeoutException:
            print(f'\n * frame not visible within {self.timeout} seconds! --> {locator[1]}')
