import logging
import os

import duckdb


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filemode="a",
    )


class DuckDBConnection:

    def __init__(self, s3_access_key_id, s3_secret_access_key):
        self.conn = duckdb.connect(database=":memory:", read_only=False)
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key

        self.conn.execute(
            """
            install httpfs;
            load httpfs;
            """
        )
        self.conn.execute(
            f"""
            set s3_endpoint='nyc3.digitaloceanspaces.com';
            set s3_region='nyc3';
            set s3_access_key_id='{self.s3_access_key_id}';
            set s3_secret_access_key='{self.s3_secret_access_key}';
            """
        )

    def close(self):
        self.conn.close()
