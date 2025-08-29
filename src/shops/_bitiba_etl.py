import re
import json
import math
import time
import random
import pandas as pd
import requests

from ..etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, before_sleep_log
MIN_WAIT_BETWEEN_REQ = 10
MAX_WAIT_BETWEEN_REQ = 15
MAX_RETRIES = 5


class ScrapingError(Exception):
    pass


class BitibaETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "Bitiba"
        self.BASE_URL = "https://www.bitiba.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = 'main#page-content'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 1
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 2
        self.with_proxy = False

    @retry(
        wait=wait_exponential(
            multiplier=1, min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(ScrapingError),
        before_sleep=before_sleep_log(logger, "WARNING"),
        reraise=True,
    )
    def _fetch_json_with_retry(self, url):
        response = requests.get(url)
        if response.status_code != 200:
            raise ScrapingError(
                f"Failed to fetch: {url} | Status: {response.status_code}")
        try:
            return response.json()
        except Exception as e:
            raise ScrapingError(f"Failed to parse JSON from {url}: {e}")

    def extract(self, category):
        urls = []
        base_api_url = (
            "https://www.bitiba.co.uk/api/discover/v1/products/list-faceted-partial"
            "?&path={category}&domain=bitiba.co.uk&language=en&page={page}&size=24"
            "&ab=shop-10734_shop_product_catalog_api_enabled_targeted_delivery.enabled"
            "%2Bidpo-1141_article_based_product_cards_targeted_delivery.on"
            "%2Bshop-11393_disable_plp_spc_api_cache_targeted_delivery.on"
            "%2Bshop-11371_enable_sort_by_unit_price_targeted_delivery.on"
            "%2Bidpo-1390_rebranding_foundation_targeted_delivery.on"
            "%2Bexplore-3092-price-redesign_targeted_delivery.on"
        )

        def build_url(page):
            return base_api_url.format(category=category, page=page)

        first_url = build_url(1)
        logger.info(f"Accessing: {first_url}")

        try:
            product_data = self._fetch_json_with_retry(first_url)
        except ScrapingError as e:
            logger.error(str(e))
            return pd.DataFrame(columns=["shop", "url"])

        pagination = product_data.get("pagination")
        if not isinstance(pagination, dict):
            logger.error(
                "'pagination' is missing or not a dict in response JSON.")

            products = product_data.get('productList', {}).get('products', [])
            urls.extend([
                self.BASE_URL.rstrip('/') + product['path']
                for product in products
                if product.get('path')
            ])

            logger.info(
                f"Extracted {len(urls)} product URLs from fallback (no pagination).")
            df = pd.DataFrame({"url": urls})
            df.insert(0, "shop", self.SHOP)
            return df

        n_pagination = pagination.get('count', 0)
        n_products_text = product_data['productList']["productListHeading"]["totalProductsText"]
        n_products = int(re.search(r'of (\d+)', n_products_text).group(1))

        logger.info(
            f"Found {n_products} products across {n_pagination} pages.")

        time.sleep(random.uniform(10, 15))

        for page in range(1, n_pagination + 1):
            page_url = build_url(page)
            logger.info(f"Accessing page {page}: {page_url}")

            try:
                data_product = self._fetch_json_with_retry(page_url)
                products = data_product.get(
                    'productList', {}).get('products', [])
                urls.extend([
                    self.BASE_URL.rstrip('/') + product['path']
                    for product in products
                    if product.get('path')
                ])
            except ScrapingError as e:
                logger.warning(f"Skipping page {page}: {str(e)}")
                continue

            time.sleep(random.uniform(10, 15))

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        logger.info(f"Total extracted URLs: {len(df)}")
        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_data = json.loads(soup.select(
                "script[type*='application/ld+json']")[1].text)
            product_title = product_data["name"]
            description = product_data["description"]
            rating = '0/5'
            if "aggregateRating" in product_data.keys():
                rating = product_data["aggregateRating"]["ratingValue"]
                rating = f"{rating}/5"

            product_url = url.replace(self.BASE_URL, "")

            # Placeholder for variant details
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            for variant in product_data['hasVariant']:
                variants.append(variant['name'].replace(
                    product_title, '').strip())
                image_urls.append(
                    soup.find('meta', attrs={'property': "og:image"}).get('content'))
                price = 0
                discount_price = 0
                discount_percentage = 0

                price_specifications = variant['offers']['priceSpecification']

                list_price = None
                sale_price = None

                for spec in price_specifications:
                    if spec.get('priceType') == 'https://schema.org/ListPrice':
                        list_price = spec['price']
                    elif spec.get('priceType') == 'https://schema.org/SalePrice':
                        sale_price = spec['price']

                if list_price is not None:
                    price = list_price
                    discount_price = sale_price if sale_price is not None else 0
                else:
                    price = sale_price if sale_price is not None else 0
                    discount_price = 0

                if discount_price != 0 and price != 0:
                    discount_percentage = "{:.2f}".format(
                        (price - discount_price) / price)

                prices.append(price)
                discounted_prices.append(discount_price)
                discount_percentages.append(discount_percentage)

            df = pd.DataFrame({
                "variant": variants,
                "price": prices,
                "discounted_price": discounted_prices,
                "discount_percentage": discount_percentages,
                "image_urls": image_urls
            })
            df.insert(0, "url", product_url)
            df.insert(0, "description", description)
            df.insert(0, "rating", rating)
            df.insert(0, "name", product_title)
            df.insert(0, "shop", self.SHOP)

            return df
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
