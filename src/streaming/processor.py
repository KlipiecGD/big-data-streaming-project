import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.config.config import config
from src.logging_utils.logger import logger

load_dotenv()

# Define the schema for the incoming JSON data
SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("market_cap_rank", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True),
])

def get_spark_session() -> SparkSession:
    """
    Create and return a SparkSession.
    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName("CryptoRealTimeProcessor") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
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

def run_processor() -> None:
    """Run the streaming data processor."""
    # Initialize Spark session
    spark = get_spark_session()
    # Get data directory from config
    data_path = config.get_paths.get("data_dir", "data")
    logger.info(f"Starting streaming processor. Monitoring data at: {data_path}")
    
    # .option("multiLine", "true") is vital for JSON arrays
    raw_stream = spark.readStream \
        .schema(SCHEMA) \
        .option("multiLine", "true") \
        .json(data_path)

    transformed_df = transform_data(raw_stream)

    query = transformed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", config.get_paths.get("checkpoint_dir", "checkpoints/")) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_processor()