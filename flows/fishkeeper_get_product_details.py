from src.factory import SHOPS, run_etl
from src.connection import Connection
from prefect import flow, task
import datetime as dt
import sys
from pathlib import Path

# Allow importing from the src directory
sys.path.append(str(Path(__file__).parent.parent))
# sys.path.append(str(Path(__file__).parent.parent / "src"))


SHOP_NAME = "FishKeeper"
RUN_DATE = dt.datetime.now().strftime("%Y%m%d")
client = run_etl(SHOP_NAME)


@task(
    name="Get Product Details",
    description="Get details of each product.",
    task_run_name=f"get-{SHOP_NAME}-urls-as-of-{RUN_DATE}"
)
async def get_product_details():
    await client.get_product_infos()


@flow
async def pipeline():
    await get_product_details()


if __name__ == "__main__":
    pipeline()
