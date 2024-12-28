import datetime
import logging
import os
import ssl

import pandas as pd

from utils import DuckDBConnection, setup_logging

S3_DATE_FORMAT = "%Y%m%d"
setup_logging()


ssl._create_default_https_context = ssl._create_unverified_context
logger = logging.getLogger(__name__)


def get_most_recent_s3_date(duckdb_con: DuckDBConnection) -> datetime.date:
    max_date = duckdb_con.sql(
        f"""select
            max(make_date(file[44:47]::int, file[48:49]::int, file[50:51]::int)) as max_date
        from glob('s3://box-office-tracking/boxofficemojo_ytd_*');"""
    )
    return_val = max_date.fetchnumpy()["max_date"][0].astype(datetime.date).date()
    return return_val


def load_current_worldwide_box_office_to_s3(duckdb_con: DuckDBConnection) -> None:
    logger.info("Starting extraction.")
    try:
        df = pd.read_html("https://www.boxofficemojo.com/year/world/")[0]
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return

    box_office_data_table_name = (
        f"boxofficemojo_ytd_{datetime.datetime.today().strftime(S3_DATE_FORMAT)}"
    )
    box_office_data_file_name = f"{box_office_data_table_name}.json"
    s3_file = f"s3://box-office-tracking/{box_office_data_table_name}.parquet"

    with open(box_office_data_file_name, "w") as file:
        df.to_json(file, orient="records")

    duckdb_con.execute(
        f"copy (select * from read_json_auto('{box_office_data_table_name}.json')) to '{s3_file}';"
    )
    row_count = f"select count(*) from '{s3_file}';"
    logger.info(
        f"Updated {s3_file} with {duckdb_con.sql(row_count).fetchnumpy()['count_star()'][0]} rows."
    )
    os.remove(box_office_data_file_name)

    return


def extract_worldwide_box_office_data() -> None:
    duckdb_con = DuckDBConnection(
        s3_access_key_id=os.getenv("BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID"),
        s3_secret_access_key=os.getenv("BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY"),
    ).conn
    if get_most_recent_s3_date(duckdb_con) < datetime.date.today():
        logger.info("Loading new worldwide box office data to s3")
        load_current_worldwide_box_office_to_s3(duckdb_con)
    else:
        logger.info("No new worldwide box office data to load to s3")


if __name__ == "__main__":
    extract_worldwide_box_office_data()
