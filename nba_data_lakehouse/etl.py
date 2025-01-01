import logging
import os

from dotenv import load_dotenv

from utils import DuckDBConnection, setup_logging

from .nba_utils.endpoint_config import ENDPOINT_EXTRACTOR_MAP
from .nba_utils.extract import get_endpoint_class
from .nba_utils.load import load_data_from_endpoint

load_dotenv()
setup_logging()


def update_nba_data(incremental: bool = False) -> None:
    if not incremental:
        seasons = list(str(i) for i in range(1946, 2025))
        logging.info(f'Running full refresh from {seasons[0]} to {seasons[-1]}')
    else:
        seasons = ['2024']
        logging.info(f'Running incrementally for {seasons[0]}')

    duckdb_con = DuckDBConnection(
        s3_access_key_id=os.getenv('NBA_DATA_LAKEHOUSE_S3_ACCESS_KEY_ID'),
        s3_secret_access_key=os.getenv('NBA_DATA_LAKEHOUSE_S3_SECRET_ACCESS_KEY'),
    ).get_connection()

    for endpoint in ENDPOINT_EXTRACTOR_MAP.keys():
        logging.info(f'Processing {endpoint}')

        endpoint_class = get_endpoint_class(endpoint, seasons)
        load_data_from_endpoint(duckdb_con, endpoint_class)

        logging.info(f'Processed {endpoint}')

    duckdb_con.close()
    logging.info('Finished processing all endpoints')


if __name__ == '__main__':
    update_nba_data(incremental=True)
