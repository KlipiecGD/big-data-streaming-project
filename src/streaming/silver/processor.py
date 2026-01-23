import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession

from src.config.config import config
from src.logging_utils.logger import logger
from src.schemas.crypto_schema import CRYPTO_BRONZE_SCHEMA
from src.streaming.silver.transformations import clean_bronze_data

load_dotenv()


def get_spark_session() -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.
    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder.appName("CryptoSilverLayerProcessor") \
        .config(
            "spark.jars.packages",
            (
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.43.1,"
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5,"
            ),
        ) \
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        ) \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ) \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def run_silver_processor() -> None:
    """Run the silver layer processor that reads from bronze and writes to silver as Parquet."""
    # Initialize Spark session
    spark = get_spark_session()
    
    # Define paths
    gcs_bucket = config.get_cloud_settings.get("gcs_bucket_name", "default-bucket")
    bronze_dir = config.get_paths_settings.get("bronze_layer_dir", "crypto_bronze")
    silver_dir = config.get_paths_settings.get("silver_layer_dir", "crypto_silver")
    
    bronze_path = f"gs://{gcs_bucket}/{bronze_dir}/"
    silver_path = f"gs://{gcs_bucket}/{silver_dir}/"
    checkpoint_dir = f"gs://{gcs_bucket}/{config.get_paths_settings.get('checkpoint_dir', 'checkpoints')}/silver/"

    logger.info(f"Starting silver layer processor")
    logger.info(f"Reading from bronze: {bronze_path}")
    logger.info(f"Writing to silver: {silver_path}")
    logger.info(f"Using checkpoint directory: {checkpoint_dir}")

    # Read streaming data from bronze layer
    raw_stream = spark.readStream \
        .schema(CRYPTO_BRONZE_SCHEMA) \
        .option("multiLine", "true") \
        .json(bronze_path)

    # Clean the data
    cleaned_stream = clean_bronze_data(raw_stream)
    
    # Write to silver layer as Parquet, partitioned by date
    query = cleaned_stream.writeStream \
        .format("parquet") \
        .option("path", silver_path) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode("append") \
        .partitionBy("date") \
        .start()
    
    logger.info("Silver layer pipeline started - writing cleaned data as Parquet")

    # Wait for the query to terminate
    query.awaitTermination()


if __name__ == "__main__":
    run_silver_processor()