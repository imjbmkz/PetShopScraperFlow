import asyncio
import json
import pandas as pd
from functions.etl import PetProductsETL
from bs4 import BeautifulSoup


class LilysKitchenETL(PetProductsETL):
    def __init__(self):
        super().__init__()
        self.SHOP = "LilysKitchen"
        self.BASE_URL = "https://www.lilyskitchen.co.uk"
        self.SELECTOR_SCRAPE_PRODUCT_INFO = 'div.l-pdp-product_primary_info'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 1
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 3

    def extract(self, category):
        category_link = f"{self.BASE_URL}{category}"
        soup = asyncio.run(self.scrape(category_link, 'main.js-main'))
        if soup:
            script_data = None

            script_tags = soup.find_all("script")
            for script_tag in script_tags:
                script_tag_content = script_tag.text.strip()
                if script_tag.text.startswith("pageContext = {"):
                    script_data = script_tag_content.replace(
                        "pageContext = ", "")

                    script_data = script_data[:-1]
                    break

            if script_data:
                product_data = json.loads(script_data)
                product_lists = product_data["analytics"]["listing"]["items"]
                df = pd.DataFrame(product_lists)[["url"]]
                df["url"] = self.BASE_URL + df["url"]
                df["shop"] = self.SHOP

                return df

    def transform(self, soup: BeautifulSoup, url: str):
        if soup:
            script_data = None

            # Check which script tag holds the product data
            script_tags = soup.find_all("script")
            for script_tag in script_tags:
                script_tag_content = script_tag.text.strip()
                if script_tag.text.startswith("pageContext = {"):
                    script_data = script_tag_content.replace(
                        "pageContext = ", "")
                    # remove semicolon in the last character
                    script_data = script_data[:-1]
                    break

            # Parse the data into dataframe
            if script_data:
                # Parse product data
                product_data = json.loads(script_data)["analytics"]["product"]
                if isinstance(product_data, list):
                    df = pd.DataFrame(product_data)
                else:
                    df = pd.DataFrame([product_data])

                # Parse product rating
                rating = json.loads(soup.select(
                    "script[type*='application/ld+json']")[1].text)
                if "aggregateRating" in rating.keys():
                    rating_value = rating["aggregateRating"]["ratingValue"]
                    rating_value = f"{rating_value}/5"
                else:
                    rating_value = None

                df["rating"] = rating_value

                # Reformat dataframe
                df = df[["name", "rating", "description", "url",
                         "unit_price", "unit_sale_price"]].copy()
                df.rename(
                    {"unit_price": "price", "unit_sale_price": "discounted_price"}, axis=1, inplace=True)

                # Additional columns
                if df["price"].values[0]:
                    df["discount_percentage"] = (
                        df["price"] - df["discounted_price"]) / df["price"]

                df['image_urls'] = soup.find_all(
                    'div', class_="js-p-mainimage")[0].find('noscript').find('img').get('src')
                df["shop"] = self.SHOP

                return df
