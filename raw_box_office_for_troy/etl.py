import json
import logging
import os

from gspread import Worksheet, service_account_from_dict
from pandas import DataFrame

from utils import DuckDBConnection, setup_logging

setup_logging()

S3_DATE_FORMAT = '%Y-%m-%d'


def pull_data_from_s3() -> DataFrame:
    duckdb_con = DuckDBConnection(
        s3_access_key_id=os.getenv('BOX_OFFICE_TRACKING_S3_ACCESS_KEY_ID'),
        s3_secret_access_key=os.getenv('BOX_OFFICE_TRACKING_S3_SECRET_ACCESS_KEY'),
    ).get_connection()

    duckdb_con.execute(
        f'''
        create or replace table box_office_mojo_dump as (
            with all_data as (
                select
                    *
                    , split_part(split_part(filename, 'release_year=', 2), '/', 1) as year_part_from_s3
                    , strptime(split_part(split_part(filename, 'scraped_date=', 2), '/', 1), '{S3_DATE_FORMAT}') as scraped_date_from_s3
                from read_parquet('s3://box-office-tracking/release_year=*/scraped_date=*/data.parquet', filename=true)
            )

            select
                "Release Group" as title
                , coalesce(try_cast(replace("Worldwide"[2:], ',', '') as integer), 0) as revenue
                , coalesce(try_cast(replace("Domestic"[2:], ',', '') as integer), 0) as domestic_rev
                , coalesce(try_cast(replace("Foreign"[2:], ',', '') as integer), 0) as foreign_rev
                , scraped_date_from_s3 as loaded_date
                , year_part_from_s3 as year_part
            from all_data
        );

        create or replace table drafter as (
            select
                round
                , overall as overall_pick
                , name
                , movie
            from read_csv_auto('raw_box_office_for_troy/assets/box_office_draft.csv')
        );

        create or replace table manual_adds as (
            select
                title
                , try_cast(revenue as integer) as revenue
                , try_cast(domestic_rev as integer) as domestic_rev
                , try_cast(foreign_rev as integer) as foreign_rev
                , try_cast(release_date as date) as first_seen_date
            from read_csv_auto('raw_box_office_for_troy/assets/manual_adds.csv')
        );

        create or replace table exclusions as (
            select
                title
            from read_csv_auto('raw_box_office_for_troy/assets/exclusions.csv')
        );

        create or replace table raw_box_office_for_troy as (
            with raw_data_and_manual_adds as (
                select
                    title
                    , revenue
                    , domestic_rev
                    , foreign_rev
                    , loaded_date
                from box_office_mojo_dump
                where
                    year_part = 2025
                    and title not in (select distinct title from manual_adds)
                    and title not in (select distinct title from exclusions)

                union all

                select
                    title
                    , revenue
                    , domestic_rev
                    , foreign_rev
                    , first_seen_date as loaded_date
                from manual_adds
            )

            select
                raw_.title
                , raw_.revenue
                , raw_.domestic_rev
                , raw_.foreign_rev
                , raw_.loaded_date
                , drafter.round as round_drafted
                , drafter.overall_pick
                , drafter.name
            from raw_data_and_manual_adds as raw_
            left join drafter
                on raw_.title = drafter.movie
        );
        '''
    )

    df = (
        duckdb_con.query('select * from raw_box_office_for_troy')
        .fetchdf()
        .astype(str)
        .replace('nan', None)
    )

    logging.info(f'Read {len(df)} rows with query from s3 bucket')

    duckdb_con.close()

    return df


def load_data_to_sheet(df: DataFrame):
    credentials_dict = json.loads(
        os.getenv('BOX_OFFICE_TRACKING_GSPREAD_CREDENTIALS').replace('\n', '\\n')
    )
    gc = service_account_from_dict(credentials_dict)
    sh = gc.open('2025 Raw Box Office Draft Data for Troy')
    worksheet = sh.worksheet('raw_data')
    worksheet.clear()
    worksheet.update([df.columns.tolist()] + df.values.tolist())
