import argparse
import logging

import modal
from modal.runner import deploy_app

from box_office_tracking.etl import extract_worldwide_box_office_data

app = modal.App('chrono')
logger = logging.getLogger(__name__)

modal_image = modal.Image.debian_slim(python_version='3.10').poetry_install_from_file(
    poetry_pyproject_toml='pyproject.toml'
)


@app.function(
    image=modal_image,
    schedule=modal.Cron('0 4 * * *'),
    secrets=[modal.Secret.from_name('chrono-secrets')],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def main():
    extract_worldwide_box_office_data()


if __name__ == '__main__':
    logger.info('Running chrono locally.')
    main.local()
    logger.info('Chrono finished running locally.')
