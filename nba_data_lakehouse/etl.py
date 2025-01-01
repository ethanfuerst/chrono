# main method to run the whole etl
# optiion to run incrementally (default) or full refresh
# incremental runs replace the current season of data
# full refresh replaces everything
# start with full refresh, then add incremental part


def update_nba_data() -> None:
    duckdb_con = DuckDBConnection(
        s3_access_key_id=os.getenv('NBA_DATA_LAKEHOUSE_S3_ACCESS_KEY_ID'),
        s3_secret_access_key=os.getenv('NBA_DATA_LAKEHOUSE_S3_SECRET_ACCESS_KEY'),
    ).get_connection()


if __name__ == '__main__':
    update_nba_data()

# %%
