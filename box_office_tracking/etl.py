import datetime
import os
import ssl
from logging import getLogger

from pandas import DataFrame, read_html

from utils import DuckDBConnection, setup_logging

S3_DATE_FORMAT = "%Y%m%d"
setup_logging()


ssl._create_default_https_context = ssl._create_unverified_context
logger = getLogger(__name__)


def load_worldwide_box_office_to_s3(duckdb_con: DuckDBConnection, year: int) -> None:
    logger.info(f"Starting extraction for {year}.")

    try:
        df = read_html(f"https://www.boxofficemojo.com/year/world/{year}")[0]
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return

    formatted_date = datetime.date.today().strftime(S3_DATE_FORMAT)

    box_office_data_table_name = f"boxofficemojo_{year}_{formatted_date}"
    box_office_data_file_name = f"{box_office_data_table_name}.json"
    s3_file = f"s3://box-office-tracking/{box_office_data_table_name}.parquet"

    with open(box_office_data_file_name, "w") as file:
        df.to_json(file, orient="records")

    duckdb_con.execute(
        f'copy (select * from read_json_auto("{box_office_data_table_name}.json")) to "{s3_file}";'
    )
    row_count = f'select count(*) from "{s3_file}";'
    logger.info(
        f'Updated {s3_file} with {duckdb_con.sql(row_count).fetchnumpy()["count_star()"][0]} rows.'
    )
    os.remove(box_office_data_file_name)


def extract_worldwide_box_office_data() -> None:
    duckdb_con = DuckDBConnection(
        s3_access_key_id=os.getenv("BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID"),
        s3_secret_access_key=os.getenv("BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY"),
    ).get_connection()

    current_year = datetime.date.today().year
    last_year = current_year - 1

    load_worldwide_box_office_to_s3(duckdb_con, current_year)
    load_worldwide_box_office_to_s3(duckdb_con, last_year)


if __name__ == "__main__":
    extract_worldwide_box_office_data()
