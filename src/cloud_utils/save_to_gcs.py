import json
from google.cloud import storage

from src.logging_utils.logger import logger

def save_json_to_gcs(data: dict, bucket_name: str, destination_blob_name: str) -> None:
    """
    Uploads a json to Google Cloud Storage.

    Args:
        data (dict): The json data to be uploaded.
        bucket_name (str): The name of the GCS bucket.
        destination_blob_name (str): The destination path in the GCS bucket.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(
            data=json.dumps(data),
            content_type='application/json'
        )

        logger.info(f"Json uploaded to {destination_blob_name} in bucket {bucket_name}.")
    except Exception as e:
        logger.error(f"Failed to upload json to GCS: {e}")
        raise