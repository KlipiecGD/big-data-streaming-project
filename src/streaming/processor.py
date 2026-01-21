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
                "org.postgresql:postgresql:42.7.4"
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
    Transform the incoming DataFrame by parsing timestamps and filtering invalid data.
    Args:
        df (DataFrame): Input DataFrame with raw data
    Returns:
        DataFrame: Transformed DataFrame with additional columns
    """
    return df.withColumn(
        "last_updated_ts", 
        to_timestamp(col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    ).withColumn("processed_at", current_timestamp()) \
     .filter(col("current_price").isNotNull())

def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write each micro-batch to PostgreSQL.
    Args:
        batch_df (DataFrame): The micro-batch DataFrame
        batch_id (int): The batch ID
    """
    jdbc_url = f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    batch_df.write.jdbc(
        url=jdbc_url,
        table=config.get_db_settings.get("table_name", "crypto_data"),
        mode="append",
        properties=properties
    )

def write_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write each micro-batch to Google BigQuery.
    Args:
        batch_df (DataFrame): The micro-batch DataFrame
        batch_id (int): The batch ID
    """
    batch_df.write.format("bigquery").option(
        "table",
        f"{config.get_cloud_settings.get('bigquery_dataset', 'crypto_dataset')}.{config.get_cloud_settings.get('bigquery_table', 'crypto_data')}"
    ).mode("append").save()

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