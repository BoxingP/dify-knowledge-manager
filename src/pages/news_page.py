from src.pages.locators import NewsPageLocators
from src.pages.page import Page


class NewsPage(Page):
    def __init__(self, driver):
        super(NewsPage, self).__init__(driver)
        self.locator = NewsPageLocators

    def extract_news(self, url: str):
        self.open_page(url, wait_string_in_url='/we3/', wait_element=self.locator.logo_img)
        self._wait_frame_to_be_visible(*self.locator.news_frame)
        self._wait_element_to_be_visible(*self.locator.headline)
        summary = self._find_element(*self.locator.summary).text.strip()
        details = self._find_element(*self.locator.details).text.strip()

        return summary, details
