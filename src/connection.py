import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text, inspect, URL
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

class Connection:
    def __init__(self, db_type="mysql", database=None):
        self.db_type = db_type.lower()
        self.database = database

        if self.db_type == "mysql":
            self.host = os.getenv("MYSQL_HOST", "localhost")
            self.port = os.getenv("MYSQL_PORT", "3306")
            self.user = os.getenv("MYSQL_USER")
            self.password = os.getenv("MYSQL_PASS")
            self.database = database or os.getenv("MYSQL_DB")
            self.driver = os.getenv("MYSQL_DRIVER", "mysql+pymysql")

        elif self.db_type == "postgres":
            self.host = os.getenv("POSTGRESQL_HOST", "localhost")
            self.port = os.getenv("POSTGRESQL_PORT", "5432")
            self.user = os.getenv("POSTGRESQL_USER")
            self.password = os.getenv("POSTGRESQL_PASS")
            self.database = database or os.getenv("POSTGRESQL_DATABASE")
            self.driver = os.getenv("POSTGRESQL_DRIVER", "postgresql+psycopg2")

        else:
            raise ValueError("db_type must be either 'mysql' or 'postgres'")

        self.engine = self._create_engine()

    def _create_engine(self) -> Engine:
        try:
            connection_string = URL.create(
                drivername=self.driver,
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            engine = create_engine(
                connection_string,
                # f"{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
                echo=False
            )

            return engine
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def execute_query(self, sql: str) -> None:
        logger.info(f"Running query: {sql}")
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def get_sql_from_file(self, file_name: str) -> str:
        file_path = Path(__file__).parent / "sql" / file_name
        # file_path = os.path.join("sql", file_name)
        try:
            with open(file_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"SQL file not found: {file_path}")
            raise

    def update_url_scrape_status(self, pkey: int, status: str, table: str, timestamp: str) -> None:
        sql = self.get_sql_from_file("update_url_scrape_status.sql")
        formatted_sql = sql.format(
            status=status, timestamp=timestamp, table_name=table, pkey=pkey)
        self.execute_query(formatted_sql)

    def extract_from_sql(self, sql: str) -> pd.DataFrame:
        try:
            return pd.read_sql(sql, self.engine)

        except Exception as e:
            logger.error(e)
            raise e

    def check_table_exists(self, table):
        inspector = inspect(self.engine)
        return inspector.has_table(table, schema=None)

    def df_to_sql(self, data: pd.DataFrame, table_name: str):
        try:
            n = data.shape[0]
            data.to_sql(table_name, self.engine,
                        if_exists="append", index=False)
            logger.info(
                f"Successfully loaded {n} records to the {table_name}.")

        except Exception as e:
            logger.error(e)
            raise e
