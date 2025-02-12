import argparse
import logging

import modal
from modal.runner import deploy_app

from nba_data_lakehouse.etl import update_nba_data
from raw_box_office_for_troy.etl import load_data_to_sheet, pull_data_from_s3
from utils import setup_logging

setup_logging()

app = modal.App('chrono')

modal_image = modal.Image.debian_slim(python_version='3.10').poetry_install_from_file(
    poetry_pyproject_toml='pyproject.toml'
)
CHRONO_SECRETS = modal.Secret.from_name('chrono-secrets')


# @app.function(
#     image=modal_image,
#     schedule=modal.Cron('0 6,18 * * *'),
#     secrets=[CHRONO_SECRETS],
#     retries=modal.Retries(
#         max_retries=3,
#         backoff_coefficient=1.0,
#         initial_delay=60.0,
#     ),
# )
# def nba_data(full_refresh: bool = False):
#     logging.info('Updating NBA data.')
#     update_nba_data(full_refresh=full_refresh)
#     logging.info('NBA data updated.')


@app.function(
    image=modal_image,
    schedule=modal.Cron('5 4 * * *'),
    secrets=[CHRONO_SECRETS],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def box_office_data():
    logging.info('Pulling box office data from S3.')
    df = pull_data_from_s3()
    logging.info('Box office data pulled from S3.')
    logging.info('Loading box office data to sheet.')
    load_data_to_sheet(df)
    logging.info('Box office data loaded to sheet.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run specific functions of the chrono app.'
    )
    parser.add_argument(
        '--function',
        choices=['nba_data', 'box_office_data'],
        required=True,
        help='The function to run.',
    )
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Run function with full refresh, if possible.',
    )

    args = parser.parse_args()

    if args.function == 'nba_data':
        if args.full_refresh:
            logging.info('Running nba_data function locally with full refresh.')
            nba_data.local(full_refresh=True)
        else:
            logging.info('Running nba_data function locally with incremental run.')
            nba_data.local(full_refresh=False)
    elif args.function == 'box_office_data':
        logging.info('Running box_office_data function locally.')
        box_office_data.local()

    logging.info('Chrono finished running locally.')
