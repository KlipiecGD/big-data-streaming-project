import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from src.config.config import config
from src.logging_utils.logger import logger
from src.schemas.crypto_schema import CRYPTO_PRICES_SCHEMA

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
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transform the incoming DataFrame by parsing timestamps, filtering invalid data and performing additional transformations.
    Args:
        df (DataFrame): Input DataFrame with raw data
    Returns:
        DataFrame: Transformed DataFrame with additional columns
    """
    # Filter out records with null 'current_price' and add processing timestamp
    df = df.withColumn(
        "last_updated_ts", 
        to_timestamp(col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    ).withColumn("processed_at", current_timestamp()) \
    .filter(col("current_price").isNotNull())

    return df

def write_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write each micro-batch to Google BigQuery.
    Args:
        batch_df (DataFrame): The micro-batch DataFrame
        batch_id (int): The batch ID
    """
    batch_df.write.format("bigquery") \
        .option("temporaryGcsBucket", config.get_cloud_settings.get("gcs_bucket_name")) \
        .option("table", f"{config.get_cloud_settings.get('bq_dataset')}.{config.get_cloud_settings.get('bq_table')}") \
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

    transformed_df = transform_data(raw_stream)

    query = transformed_df.writeStream \
        .foreachBatch(write_to_bigquery) \
        .option("checkpointLocation", config.get_paths.get("checkpoint_dir", "checkpoints/")) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_processor()