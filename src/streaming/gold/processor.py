import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from src.config.config import config
from src.logging_utils.logger import logger
from src.schemas.crypto_schema import CRYPTO_SILVER_SCHEMA

from src.streaming.gold.transformations import (
    transform_main_data,
    transform_rolling_average,
)

load_dotenv()


def get_spark_session() -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.
    Returns:
        SparkSession: Configured Spark session
    """
    return (
        SparkSession.builder.appName("CryptoGoldLayerProcessor")
        .config(
            "spark.jars.packages",
            (
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.43.1,"
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5,"
            ),
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def write_main_data_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write each micro-batch of main data to Google BigQuery.
    Args:
        batch_df (DataFrame): The micro-batch DataFrame
        batch_id (int): The batch ID
    """
    batch_df.write.format("bigquery").option(
        "temporaryGcsBucket", config.get_cloud_settings.get("gcs_bucket_name")
    ).option(
        "table",
        f"{config.get_cloud_settings.get('bq_dataset')}.{config.get_cloud_settings.get('bq_main_table')}",
    ).mode("append").save()


def write_rolling_avg_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write each micro-batch of rolling averages to Google BigQuery.
    Args:
        batch_df (DataFrame): The micro-batch DataFrame
        batch_id (int): The batch ID
    """
    batch_df.write.format("bigquery").option(
        "temporaryGcsBucket", config.get_cloud_settings.get("gcs_bucket_name")
    ).option(
        "table",
        f"{config.get_cloud_settings.get('bq_dataset')}.{config.get_cloud_settings.get('bq_rolling_avg_table')}",
    ).mode("append").save()


def run_gold_processor() -> None:
    """Run the gold layer processor that reads from silver and writes to BigQuery."""
    # Initialize Spark session
    spark = get_spark_session()

    # Define paths
    gcs_bucket = config.get_cloud_settings.get("gcs_bucket_name", "default-bucket")
    silver_dir = config.get_paths_settings.get("silver_layer_dir", "crypto_silver")
    silver_path = f"gs://{gcs_bucket}/{silver_dir}/"
    checkpoint_dir = f"gs://{gcs_bucket}/{config.get_paths_settings.get('checkpoint_dir', 'checkpoints')}/gold/"

    logger.info(f"Starting gold layer processor reading from {silver_path}")
    logger.info(f"Using checkpoint directory: {checkpoint_dir}")

    # Read streaming data from silver layer (Parquet)
    silver_stream = spark.readStream.schema(CRYPTO_SILVER_SCHEMA).parquet(silver_path)

    # Pipeline 1: Detailed per-coin analytics
    transformed_df = transform_main_data(silver_stream)

    query_detailed = (
        transformed_df.writeStream.foreachBatch(write_main_data_to_bigquery)
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "main/"))
        .outputMode("append")
        .start()
    )

    logger.info("Gold layer - detailed data pipeline started")

    # Pipeline 2: 2-minute rolling averages per coin
    rolling_avg_df = transform_rolling_average(silver_stream)

    query_rolling_avg = (
        rolling_avg_df.writeStream.foreachBatch(write_rolling_avg_to_bigquery)
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", os.path.join(checkpoint_dir, "rolling_avg/"))
        .outputMode("append")
        .start()
    )

    logger.info("Gold layer - rolling average pipeline started")

    # Wait for both queries to terminate
    query_detailed.awaitTermination()
    query_rolling_avg.awaitTermination()


if __name__ == "__main__":
    run_gold_processor()
