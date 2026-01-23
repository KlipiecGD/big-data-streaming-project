import requests
import time

from src.cloud_utils.save_to_gcs import save_json_to_gcs
from src.config.config import config
from src.logging_utils.logger import logger


def fetch_crypto_data() -> None:
    """
    Fetch cryptocurrency data from the API and save it Google Cloud Storage.
    """

    url = config.get_api_settings.get(
        "url", "https://api.coingecko.com/api/v3/coins/markets"
    )

    gcs_bucket_name = config.get_cloud_settings.get("gcs_bucket_name", "default-bucket")
    gcs_folder = config.get_paths_settings.get("bronze_layer_dir", "crypto_bronze")

    # Fetching data
    while True:
        try:
            # Make the API request
            logger.info(
                f"Fetching data from {url} with params {config.get_api_connection_params}"
            )
            response = requests.get(url, params=config.get_api_connection_params)

            # Raise an error for bad responses
            response.raise_for_status()
            logger.info("Data fetched successfully from the API.")

            # Save the data to a JSON file
            data = response.json()
            filename = f"{gcs_folder}/crypto_data_{int(time.time())}.json"
            save_json_to_gcs(data, gcs_bucket_name, filename)

            logger.info(f"Data saved successfully to GCS at {filename}.")
            # Fetch data every 60 seconds
            time.sleep(config.get_api_settings.get("wait_time", 60))

        except requests.RequestException as e:
            logger.error(
                f"Error fetching data: {e}. Retrying in {config.get_api_settings.get('retry_time', 10)} seconds..."
            )

            # Retry after 10 seconds
            time.sleep(config.get_api_settings.get("retry_time", 10))


if __name__ == "__main__":
    fetch_crypto_data()
