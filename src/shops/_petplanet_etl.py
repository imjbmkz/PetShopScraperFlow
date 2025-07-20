import re
import math
import random
import asyncio
import pandas as pd

from functions.etl import PetProductsETL
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
        self.SELECTOR_SCRAPE_PRODUCT_INFO = '.products-panel'
        self.MIN_SEC_SLEEP_PRODUCT_INFO = 1
        self.MAX_SEC_SLEEP_PRODUCT_INFO = 3

    async def product_list_scrolling(self, url, selector, click_times):
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
                    "Origin": "https://www.petplanet.co.uk",
                    "Referer": url,
                })

                await page.goto(url, wait_until="load")
                await page.wait_for_selector(selector, timeout=30000)

                logger.info(
                    "Starting to click 'Load More' button if available...")

                for i in range(click_times):
                    try:
                        load_more_btn = await page.query_selector("#ContentPlaceHolder1_ctl00_Shop1_ProdMenu1_LoadMoreBtn1")
                        if load_more_btn and await load_more_btn.is_visible():
                            current_products = await page.query_selector_all("a.product-name")
                            count_before = len(current_products)

                            await load_more_btn.click()
                            logger.info(
                                f"Clicked 'Load More' button ({i + 1}/{click_times})")

                            await page.wait_for_function(
                                f'document.querySelectorAll("a.product-name").length > {count_before}',
                                timeout=120000
                            )

                        else:
                            logger.warning(
                                "Load More button not found or not visible. Stopping clicks early.")
                            break
                    except Exception as e:
                        logger.warning(f"Error during click {i + 1}: {e}")
                        break

                logger.info("Scraping complete. Extracting content...")

                rendered_html = await page.content()
                logger.info(
                    f"Successfully extracted data from {url}"
                )
                sleep_time = random.uniform(
                    3, 5)
                logger.info(f"Sleeping for {sleep_time} seconds...")
                soup = BeautifulSoup(rendered_html, "html.parser")
                return soup.find_all('a', class_="product-name")

        except Exception as e:
            logger.error(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def extract(self, category):
        url = f"{self.BASE_URL}{category}"
        urls = []

        soup = asyncio.run(self.scrape(url, '.products-panel'))

        num_items_pattern = r"Showing (\d+) items"
        n_items = int(re.search(num_items_pattern, soup.text).group(1))
        n_pagination = math.ceil(n_items / 20)

        product_list = asyncio.run(self.product_list_scrolling(
            url, '.products-panel', n_pagination))

        urls = [self.BASE_URL + product["href"] for product in product_list]

        df = pd.DataFrame({"url": urls})
        df.drop_duplicates(inplace=True)
        df.insert(0, "shop", self.SHOP)

        return df

    def transform(self, soup: BeautifulSoup, url: str):
        try:
            # Get the product title, rating, and description
            product_title = soup.find("h1").text
            description = soup.find("div", id="nav-description").text
            rating = soup.find(
                "div", id="ContentPlaceHolder1_ctl00_Product1_ctl02_SummaryPanel")
            if rating:
                rating_h3 = rating.find("h3")
                rating_value = f"{rating_h3.text}/5"
            else:
                rating_value = None
            product_url = url.replace(self.BASE_URL, "")

            # Get product variants
            product_options = soup.select_one(
                "div[class*='product-option-grid']")

            # Placeholder for variant details
            variants = []
            prices = []
            discounted_prices = []
            discount_percentages = []
            image_urls = []

            if product_options:
                product_variants = product_options.find_all("a")

                # Get the variant name, price, and discounted price
                for product_variant in product_variants:
                    variant = product_variant.select_one(
                        "div[class*='h5']").text

                    response_new = self.session.get(url, verify=False)
                    soup_new = BeautifulSoup(response_new.content)

                    price = soup_new.select_one("span[class*='fw-bold fs-4']")
                    if price is None:
                        price = soup_new.select_one(
                            "div[class*='fw-bold fs-4']")

                    original_price = price.select_one("span")
                    if original_price:
                        original_price_amount = float(
                            original_price.text.replace("£", ""))
                        discounted_price_amount = float(
                            price.contents[-1].strip().replace("£", ""))
                        discount_percentage = (
                            original_price_amount - discounted_price_amount) / original_price_amount
                    else:
                        original_price_amount = float(
                            price.contents[-1].strip().replace("£", ""))
                        discounted_price_amount = None
                        discount_percentage = None

                    variants.append(variant)
                    prices.append(original_price_amount)
                    discounted_prices.append(discounted_price_amount)
                    discount_percentages.append(discount_percentage)
                    image_urls.append(', '.join([img.get('src') for img in soup.find(
                        'div', class_="product-gallery-control").find_all('img')]))

            else:
                variant = None

                price = soup.select_one("span[class*='fw-bold fs-4']")
                if price is None:
                    price = soup.select_one("div[class*='fw-bold fs-4']")

                original_price = price.select_one("span")
                if original_price:
                    original_price_amount = float(
                        original_price.text.replace("£", ""))
                    discounted_price_amount = float(
                        price.contents[-1].strip().replace("£", ""))
                    discount_percentage = (
                        original_price_amount - discounted_price_amount) / original_price_amount
                else:
                    original_price_amount = float(
                        price.contents[-1].strip().replace("£", ""))
                    discounted_price_amount = None
                    discount_percentage = None

                variants.append(variant)
                prices.append(original_price_amount)
                discounted_prices.append(discounted_price_amount)
                discount_percentages.append(discount_percentage)
                image_urls.append(', '.join([img.get('src') for img in soup.find(
                    'div', class_="product-gallery-control").find_all('img')]))

            # Compile the data acquired into dataframe
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
            df.insert(0, "rating", rating_value)
            df.insert(0, "name", product_title)
            df.insert(0, "shop", self.SHOP)

            return df

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
