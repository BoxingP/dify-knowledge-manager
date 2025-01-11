from selenium.webdriver.common.by import By


class PageLocators(object):
    body = (By.XPATH, '//body')
    html = (By.TAG_NAME, 'html')


class NewsPageLocators(PageLocators):
    logo_img = (By.XPATH, '//*[@id="logoTr"]/td/a/img')
    news_frame = (By.XPATH, '//*[@id="documentPage"]')
    headline = (By.XPATH, '//*[@id="headline"]')
    summary = (
        By.XPATH,
        '//font[@class="docSynopsisHeader" and contains(translate(text(), "SUMMARY", "summary"), "summary")]/parent::td/p/font'
    )
    details = (By.XPATH, '//*[@class="content"]//table//span[@class="contentText"]')
