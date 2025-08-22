import re
import math
import json
import asyncio
import pandas as pd

from ..etl import PetProductsETL
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright
from loguru import logger

import warnings
warnings.filterwarnings("ignore")


class PetPlanetETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "PetPlanet"
        self.BASE_URL = "https://www.petplanet.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '#MainContent'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 1
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 2

    def extract(self, category):
        current_url = f"{self.BASE_URL}{category}"
        urls = []

        soup = asyncio.run(self.scrape(current_url, '#ProductGridContainer'))
        if not soup or isinstance(soup, bool):
            return pd.DataFrame({})

        n_products = int(soup.find(
            'span', class_="js-product-count").get_text(strip=True).replace(" products", ''))
        n_pagination = math.ceil(n_products / 24)

        urls = [self.BASE_URL + product.find('a').get('href')
                for product in soup.find_all('h3', class_="card__heading")]

        for n in range(1, n_pagination + 1):
            pagination_url = current_url + f'?page={n}'
            pagination_soup = asyncio.run(self.scrape(
                pagination_url, '#ProductGridContainer', proxy=False, min_sec=0.5, max_sec=1))

            urls.extend([self.BASE_URL + product.find('a').get(
                'href') for product in pagination_soup.find_all('h3', class_="card__heading")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_name = soup.find('div', class_="product__title").find(
                'h1').get_text(strip=True)
            product_description = soup.find(
                'div', class_="product__description").get_text(strip=True)
            product_url = url.replace(self.BASE_URL, "")

            product_rating = '0/5'

            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            list_variant = json.loads(soup.find_all(
                'script', attrs={'type': 'application/json'})[-1].get_text())

            if isinstance(list_variant, dict):
                variants.append(None)
                image_urls.append(
                    soup.find('meta', attrs={'property': 'og:image'}).get('content'))

                is_discount = soup.find("div", class_="price__container").select_one(
                    "s.price-item--regular").get_text(strip=True)
                if is_discount == '':
                    price = float(soup.find("div", class_="price__container").find(
                        'span', class_="price-item--regular").get_text(strip=True).replace("£", "").replace("GBP", ""))
                    prices.append(price)
                    discounted_prices.append(None)
                    discount_percentages.append(None)
                else:
                    price = float(soup.find("div", class_="price__container").select_one(
                        "s.price-item--regular").get_text().replace("£", "").replace("GBP", ""))
                    discount_price = float(soup.find("div", class_="price__container").find(
                        'span', class_="price-item--sale").get_text().replace("£", "").replace("GBP", ""))
                    discount_percentage = "{:.2f}".format(
                        (price - discount_price) / price)

                    prices.append(price)
                    discounted_prices.append(discount_price)
                    discount_percentages.append(discount_percentage)

            else:
                for i in list_variant:
                    variants.append(i['title'])
                    image_urls.append("https:" + i['featured_image']['src'])

                    if i['compare_at_price'] != None:
                        price = i['compare_at_price'] / 100
                        discount_price = i['price'] / 100
                        discount_percentage = "{:.2f}".format(
                            (price - discount_price) / price)

                        prices.append(price)
                        discounted_prices.append(discount_price)
                        discount_percentages.append(discount_percentage)

                    else:
                        prices.append(i['price'] / 100)
                        discounted_prices.append(None)
                        discount_percentages.append(None)

            df = pd.DataFrame({
                "variant": variants,
                "price": prices,
                "discounted_price": discounted_prices,
                "discount_percentage": discount_percentages,
                "image_urls": image_urls
            })
            df.insert(0, "url", product_url)
            df.insert(0, "description", product_description)
            df.insert(0, "rating", product_rating)
            df.insert(0, "name", product_name)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
