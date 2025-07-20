import asyncio
import json
import re
import math
import pandas as pd
from functions.etl import PetProductsETL
from bs4 import BeautifulSoup
from loguru import logger


class PetSupermarketETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "PetSupermarket"
        self.BASE_URL = "https://www.pet-supermarket.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '.product-details'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 1
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 3

    def extract(self, category):
        current_url = f"{self.BASE_URL}{category}"

        urls = []

        while True:
            soup = asyncio.run(self.scrape(current_url, '.paws-content'))
            if not soup:
                logger.warning(
                    f"[WARNING] Failed to scrape: {current_url}. Ending pagination.")
                break

            try:
                n_products_text = soup.find(
                    'span', class_="total-results").get_text(strip=True)
                match = re.search(r'[\d,]+', n_products_text)
                n_products = int(match.group().replace(
                    ',', '')) if match else 0
                n_pagination = math.ceil(n_products / 24)

                urls = [product.get('href') for product in soup.find_all(
                    'a', class_="product-item-link")]

                for n in range(1, n_pagination + 1):

                    pagination_url = current_url + f'?page={n}'
                    pagination_soup = asyncio.run(
                        self.scrape(pagination_url, '.paws-content'))

                    urls.extend([product.get('href') for product in pagination_soup.find_all(
                        'a', class_="product-item-link")])

            except Exception as e:
                logger.error(
                    f"[ERROR] Failed to process page {current_url}: {e}")
                break

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)

        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            product_header = soup.select_one(
                "div[class*='product-header']")
            product_title = product_header.select_one(
                "h1[class*='name']").text
            rating = product_header.select_one(
                "div[class*='js-ratingCalc']")
            if rating:
                rating_rating = round(json.loads(
                    rating["data-rating"])["rating"], 2)
                rating_total = json.loads(rating["data-rating"])["total"]
                rating = f"{rating_rating}/{rating_total}"

            if soup.select("div[id*='product-details-tab']"):
                description_list = soup.select(
                    "div[id*='product-details-tab']")[0].select("p")
                description = " ".join(
                    [p.text for p in description_list]).strip()
            else:
                description = soup.find(
                    'meta', attrs={'name': 'description'}).get('content')

            product_url = url.replace(self.BASE_URL, "")

            # Placeholder for variant details
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []
            variant_tiles = product_header.select(
                "div[class*='variant-tile']")

            for variant_tile in variant_tiles:
                variant_tile_item = variant_tile.select_one("li")
                variant = variant_tile_item["data-product-feature-qualifier-name"]
                if variant_tile_item.has_attr("data-was-price"):
                    price = float(
                        variant_tile_item["data-was-price"].replace("£", ""))
                    discounted_price = float(
                        variant_tile_item["data-selling-price-value"].replace("£", ""))
                    discount_percentage = None
                    if price > 0:
                        discount_percentage = (
                            price - discounted_price) / price

                else:
                    price = float(
                        variant_tile_item["data-selling-price-value"])
                    discounted_price = None
                    discount_percentage = None

                carousel_divs = soup.find_all(
                    'div', attrs={'data-test': 'carousel-inner-wrapper'})
                if carousel_divs:
                    image_url = ', '.join(
                        [img.get('src') for img in carousel_divs[0].find_all('img')])
                else:
                    image_url = None

                variants.append(variant)
                prices.append(price)
                discounted_prices.append(discounted_price)
                discount_percentages.append(discount_percentage)
                image_urls.append(image_url)

            df = pd.DataFrame(
                {
                    "variant": variants,
                    "price": prices,
                    "discounted_price": discounted_prices,
                    "discount_percentage": discount_percentages,
                    "image_urls": image_urls
                }
            )

            df.insert(0, "url", product_url)
            df.insert(0, "description", description)
            df.insert(0, "rating", rating)
            df.insert(0, "name", product_title)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
