import sys
from pathlib import Path

# Allow importing from the src directory
sys.path.append(str(Path(__file__).parent.parent))
# sys.path.append(str(Path(__file__).parent.parent / "src"))

import datetime as dt
from prefect import flow, task
from src.connection import Connection
from src.factory import SHOPS, run_etl

SHOP_NAME = "Zooplus"
RUN_DATE = dt.datetime.now().strftime("%Y%m%d")
client = run_etl(SHOP_NAME)

@task(
    name="Get Product URLs",
    description="Get product URLs (new and old) from the shop.",
    task_run_name=f"get-{SHOP_NAME}-urls-as-of-{RUN_DATE}"
)
def get_product_urls():
    print("Success")

@task(
    name="Get Product Details",
    description="Get product details based on the collected URLs.",
    task_run_name=f"get-{SHOP_NAME}-product-details-as-of-{RUN_DATE}"
)
def get_product_details():
    print("Success")

@flow
def pipeline():
    get_product_urls()
    get_product_details()

if __name__ == "__main__":
    pipeline()