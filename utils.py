import logging
import os
import tempfile

import duckdb
from pandas import DataFrame


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filemode="a",
    )


class DuckDBConnection:
    def __init__(self, s3_access_key_id: str, s3_secret_access_key: str) -> None:
        self.conn = duckdb.connect(database="chrono.db", read_only=False)

        self.conn.execute(
            f"""
            install httpfs;
            load httpfs;
            CREATE SECRET (
                TYPE S3,
                KEY_ID '{s3_access_key_id}',
                SECRET '{s3_secret_access_key}',
                REGION 'nyc3',
                ENDPOINT 'nyc3.digitaloceanspaces.com'
            );
            """
        )

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return self.conn

    def close(self) -> None:
        self.conn.close()


def load_df_to_s3_table(
    duckdb_con: duckdb.DuckDBPyConnection,
    df: DataFrame,
    s3_key: str,
    bucket_name: str,
) -> int:
    logging.info(f'Loading {s3_key} to {bucket_name}')

    file_name = f'{s3_key}.json'.replace('/', '_')
    s3_file = f's3://{bucket_name}/{s3_key}.parquet'

    with open(file_name, 'w') as file:
        df.to_json(file, orient='records')

    duckdb_con.execute(
        f'copy (select * from read_json_auto("{file_name}")) to "{s3_file}";'
    )

    row_count_query = f'select count(*) from "{s3_file}";'
    rows_loaded = duckdb_con.sql(row_count_query).fetchnumpy()["count_star()"][0]

    logging.info(f'Updated {s3_file} with {rows_loaded} rows.')

    os.remove(file_name)

    return rows_loaded
