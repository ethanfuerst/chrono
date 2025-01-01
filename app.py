import argparse
import logging

import modal
from modal.runner import deploy_app

from box_office_tracking.etl import extract_worldwide_box_office_data
from nba_data_lakehouse.etl import update_nba_data
from utils import setup_logging

setup_logging()

app = modal.App('chrono')

modal_image = modal.Image.debian_slim(python_version='3.10').poetry_install_from_file(
    poetry_pyproject_toml='pyproject.toml'
)
CHRONO_SECRETS = modal.Secret.from_name('chrono-secrets')


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 4 * * *'),
    secrets=[CHRONO_SECRETS],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def main():
    logging.info('Extracting box office data.')
    extract_worldwide_box_office_data()
    logging.info('Box office data extracted.')


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 6,18 * * *'),
    secrets=[CHRONO_SECRETS],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def nba_data():
    logging.info('Updating NBA data.')
    update_nba_data(incremental=True)
    logging.info('NBA data updated.')


if __name__ == '__main__':
    logging.info('Running chrono locally.')
    main.local()
    nba_data.local()
    logging.info('Chrono finished running locally.')
