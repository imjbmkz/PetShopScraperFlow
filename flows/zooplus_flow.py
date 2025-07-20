import sys
from pathlib import Path

# Allow importing from the src directory
sys.path.append(str(Path(__file__).parent.parent))
# sys.path.append(str(Path(__file__).parent.parent / "src"))

from prefect import flow, task
from src.connection import Connection
from src.factory import SHOPS, run_etl

SHOP_NAME = "Zooplus"
client = run_etl(SHOP_NAME)

@task
def get_product_urls():
    pass

@task
def get_product_details():
    pass