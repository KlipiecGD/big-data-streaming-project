import json
import requests
import time
import os

from src.config.config import config
from src.logging_utils.logger import logger


def fetch_crypto_data(
    url: str = config.get_api_settings.get(
        "url", "https://api.coingecko.com/api/v3/coins/markets"
    ),
    save_path: str = config.get_paths.get("data_dir", "data"),
) -> None:
    """
    Fetch cryptocurrency data from the API and save it locally.
    Args:
        url (str): API endpoint URL
        save_path (str): Directory to save the fetched data
    """

    # Ensure the save directory exists
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    # Fetching data
    while True:
        try:
            response = requests.get(url, params=config.get_api_connection_params)
            response.raise_for_status()
            logger.info("Data fetched successfully from the API.")
            data = response.json()
            filename = f"crypto_data_{int(time.time())}.json"
            save_path_full = os.path.join(save_path, filename)
            logger.info(f"Saving data to {save_path_full}")
            with open(save_path_full, "w") as f:
                json.dump(data, f, indent=2)
            logger.info("Data saved successfully.")
            time.sleep(
                config.get_api_settings.get("wait_time", 60)
            )  # Fetch data every 60 seconds

        except requests.RequestException as e:
            logger.error(
                f"Error fetching data: {e}. Retrying in {config.get_api_settings.get('retry_time', 10)} seconds..."
            )
            time.sleep(
                config.get_api_settings.get("retry_time", 10)
            )  # Retry after 10 seconds


if __name__ == "__main__":
    fetch_crypto_data()
