import asyncio
import random
import time
import pandas as pd

from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright
from loguru import logger


class OcadoETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "Ocado"
        self.BASE_URL = "https://www.ocado.com"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '.main-content'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 1
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 3

    async def product_list_scrolling(self, url, selector):
        soup = None
        browser = None
        try:
            async with async_playwright() as p:
                browser_args = {
                    "headless": True,
                    "args": ["--disable-blink-features=AutomationControlled"]
                }

                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(
                    user_agent=UserAgent().random,
                    viewport={"width": random.randint(
                        1200, 1600), "height": random.randint(800, 1200)},
                    locale="en-US"
                )

                page = await context.new_page()
                await page.set_extra_http_headers({
                    "User-Agent": UserAgent().random,
                    "Accept-Language": "en-US,en;q=0.9",
                    "Origin": "https://www.ocado.com",
                    "Referer": url,
                })

                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_selector(selector, timeout=30000)

                logger.info(
                    "Starting to scrape the product list (Infinite scroll scrape)...")

                scroll_step = 300
                scroll_delay = 1

                current_position = 0
                page_height = await page.evaluate('() => document.body.scrollHeight')

                while current_position < page_height:
                    # Scroll to the current position
                    await page.evaluate(f'window.scrollTo(0, {current_position})')
                    current_position += scroll_step
                    time.sleep(scroll_delay)

                logger.info("Scraping complete. Extracting content...")

                rendered_html = await page.content()
                logger.info(
                    f"Successfully extracted data from {url}"
                )
                sleep_time = random.uniform(
                    3, 5)
                logger.info(f"Sleeping for {sleep_time} seconds...")
                soup = BeautifulSoup(rendered_html, "html.parser")
                return soup.find('ul', class_="fops-regular")

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def extract(self, category):
        category_link = f"{self.BASE_URL}{category}"
        soup = asyncio.run(self.scrape(
            category_link, '.main-column'))
        n_product = int(soup.find('div', class_="main-column").find('div',
                        class_="total-product-number").find('span').get_text().replace(' products', ''))

        list_soup_product = asyncio.run(self.product_list_scrolling(
            f"{category_link}?display={n_product}", '.fops-regular'))
        product_list = [item for item in list_soup_product.find_all(
            'li', class_="fops-item") if 'fops-item--advert' not in item.get('class', [])]
        urls = [self.BASE_URL + product.find('a').get('href')
                for product in product_list if product.find('a')]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find(
                'header', class_="bop-title").find('h1').get_text(strip=True)
            product_description = None
            if soup.find('div', class_="gn-accordionElement__wrapper"):
                product_description = soup.find(
                    'div', class_="gn-accordionElement__wrapper").find('div', class_="bop-info__content").get_text()

            product_url = url.replace(self.BASE_URL, "")
            product_rating = '0/5'

            product_rating_wrapper = soup.find('section', id='reviews').find(
                'span', attrs={'itemprop': 'ratingValue'})
            if product_rating_wrapper is not None:
                product_rating = product_rating_wrapper.get_text().strip() + '/5'

            variant = None
            price = None
            discounted_price = None
            discount_percentage = None
            image_urls = None

            if soup.find('header', class_="bop-title").find('span', class_="bop-catchWeight"):
                variant = soup.find('header', class_="bop-title").find('span',
                                                                       class_="bop-catchWeight").get_text(strip=True)

            if soup.find('span', class_="bop-price__old"):
                price_text = soup.find(
                    'span', class_="bop-price__old").get_text(strip=True)
                if '£' in price_text:
                    price = float(soup.find(
                        'span', class_="bop-price__old").get_text(strip=True).replace('£', ""))
                else:
                    price = float(soup.find(
                        'span', class_="bop-price__old").get_text(strip=True).replace('p', "")) / 100
                discounted_price = "{:.2f}".format(float(soup.find(
                    'h2', class_="bop-price__current").find('meta', attrs={'itemprop': 'price'}).get('content')))
                discount_percentage = round(
                    (price - float(discounted_price)) / price, 2)
            else:
                price = "{:.2f}".format(float(soup.find(
                    'h2', class_="bop-price__current").find('meta', attrs={'itemprop': 'price'}).get('content')))

            image_urls = self.BASE_URL + soup.find(
                'meta', attrs={'property': "og:image"}).get('content')

            df = pd.DataFrame([{
                "url": product_url,
                "description": product_description,
                "rating": product_rating,
                "name": product_name,
                "shop": self.SHOP,
                "variant": variant,
                "price": price,
                "discounted_price": discounted_price,
                "discount_percentage": discount_percentage,
                "image_urls": image_urls
            }])

            return df
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
