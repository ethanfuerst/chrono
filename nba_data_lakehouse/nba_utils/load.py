import logging
import os
import sys
from typing import Generator

from duckdb import DuckDBPyConnection
from pandas import DataFrame

from .endpoint_config import NBADataExtractor

sys.path.append(os.path.dirname(sys.path[0]))
from utils import load_df_to_s3_table, setup_logging

setup_logging()


def load_data_from_endpoint(
    duckdb_con: DuckDBPyConnection, endpoint_class: NBADataExtractor
) -> None:
    total_rows_loaded = 0
    endpoint = endpoint_class.endpoint
    for table_name, df in endpoint_class.get_data():
        logging.info(f'Starting extraction for {table_name}.')

        rows_loaded = load_df_to_s3_table(
            duckdb_con=duckdb_con,
            df=df,
            file_path=table_name,
            bucket_name='nba-data-lakehouse',
        )

        total_rows_loaded += rows_loaded

    logging.info(f'Total rows loaded to s3 for {endpoint}: {total_rows_loaded}')
