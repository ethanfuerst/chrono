import logging
import argparse
import modal
from modal.runner import deploy_app

from boxofficetracking import extract_worldwide_box_office_data

app = modal.App("chrono")
logger = logging.getLogger(__name__)

modal_image = modal.Image.debian_slim(python_version="3.10").run_commands(
    "pip install duckdb==0.10.0",
    "pip install html5lib==1.1",
    "pip install lxml==5.1.0",
    "pip install python-dotenv==1.0.1",
    "pip install pandas==2.1.4",
    "pip install requests==2.31.0",
    "pip install python-dotenv==1.0.1",
)


@app.function(
    image=modal_image,
    schedule=modal.Cron("0 4 * * *"),
    secrets=[modal.Secret.from_name("chrono-secrets")],
    retries=modal.Retries(
        max_retries=3,
        backoff_coefficient=1.0,
        initial_delay=60.0,
    ),
)
def main():
    extract_worldwide_box_office_data()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the application locally or deploy to Modal."
    )
    parser.add_argument(
        "--deploy", action="store_true", help="Deploy the application to Modal."
    )
    args = parser.parse_args()
    if args.deploy:
        logger.info("Deploying chrono to Modal.")
        deploy_app(app)
    else:
        logger.info("Running chrono locally.")
        main.local()