import logging
import os

import duckdb


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filemode="a",
    )


class DuckDBConnection:
    def __init__(self, s3_access_key_id: str, s3_secret_access_key: str) -> None:
        self.conn = duckdb.connect(database=":memory:", read_only=False)

        self.conn.execute(
            f"""
            install httpfs;
            load httpfs;
            set s3_endpoint='nyc3.digitaloceanspaces.com';
            set s3_region='nyc3';
            set s3_access_key_id='{s3_access_key_id}';
            set s3_secret_access_key='{s3_secret_access_key}';
            """
        )

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return self.conn

    def close(self) -> None:
        self.conn.close()
