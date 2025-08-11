import sys
from pathlib import Path

# Allow importing from the src directory
sys.path.append(str(Path(__file__).parent.parent))
# sys.path.append(str(Path(__file__).parent.parent / "src"))

import datetime as dt
from prefect import flow, task
from src.connection import Connection
from src.factory import SHOPS, run_etl

SHOP_NAME = "PetShopOnline"
RUN_DATE = dt.datetime.now().strftime("%Y%m%d")
client = run_etl(SHOP_NAME)

@task(
    name="Get Product URLs",
    description="Get product URLs (new and old) from the shop.",
    task_run_name=f"get-{SHOP_NAME}-urls-as-of-{RUN_DATE}"
)
def get_product_urls():
    client.get_links_by_category()

@flow
def pipeline():
    get_product_urls()

if __name__ == "__main__":
    pipeline()