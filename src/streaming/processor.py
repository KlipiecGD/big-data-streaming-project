import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from src.config.config import config
from src.logging_utils.logger import logger
from src.schemas.crypto_schema import CRYPTO_PRICES_SCHEMA

from src.streaming.transformations import transform_main_data, transform_rolling_average

load_dotenv()

def get_spark_session() -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.
    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder.appName("CryptoRealTimeProcessor") \
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
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def write_main_data_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write each micro-batch of main data to Google BigQuery.
    Args:
        batch_df (DataFrame): The micro-batch DataFrame
        batch_id (int): The batch ID
    """
    batch_df.write.format("bigquery") \
        .option("temporaryGcsBucket", config.get_cloud_settings.get("gcs_bucket_name")) \
        .option("table", f"{config.get_cloud_settings.get('bq_dataset')}.{config.get_cloud_settings.get('bq_main_table')}") \
        .mode("append") \
        .save()
    
def write_rolling_avg_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write each micro-batch of rolling averages to Google BigQuery.
    Args:
        batch_df (DataFrame): The micro-batch DataFrame
        batch_id (int): The batch ID
    """
    batch_df.write.format("bigquery") \
        .option("temporaryGcsBucket", config.get_cloud_settings.get("gcs_bucket_name")) \
        .option("table", f"{config.get_cloud_settings.get('bq_dataset')}.{config.get_cloud_settings.get('bq_rolling_avg_table')}") \
        .mode("append") \
        .save()

def run_processor() -> None:
    """Run the streaming data processor."""
    # Initialize Spark session
    spark = get_spark_session()
    # Get data directory from config
    data_path = config.get_paths.get("data_dir", "data")
    logger.info(f"Starting streaming processor. Monitoring data at: {data_path}")
    
    # Read streaming data
    raw_stream = spark.readStream \
        .schema(CRYPTO_PRICES_SCHEMA) \
        .option("multiLine", "true") \
        .json(data_path)

    # Pipeline 1: Detailed per-coin analytics 
    transformed_df = transform_main_data(raw_stream)
    
    query_detailed = transformed_df.writeStream \
        .foreachBatch(write_main_data_to_bigquery) \
        .option("checkpointLocation", os.path.join(config.get_paths.get("checkpoint_dir", "checkpoints/"), "main/")) \
        .outputMode("append") \
        .start()
    
    logger.info("Detailed data pipeline started")

    # Pipeline 2: 2-minute rolling averages per coin
    rolling_avg_df = transform_rolling_average(raw_stream)
    
    query_rolling_avg = rolling_avg_df.writeStream \
        .foreachBatch(write_rolling_avg_to_bigquery) \
        .option("checkpointLocation", os.path.join(config.get_paths.get("checkpoint_dir", "checkpoints/"), "rolling_avg/")) \
        .outputMode("append") \
        .start()
    
    logger.info("Rolling average pipeline started")

    # Wait for both queries to terminate
    query_detailed.awaitTermination()
    query_rolling_avg.awaitTermination()

if __name__ == "__main__":
    run_processor()