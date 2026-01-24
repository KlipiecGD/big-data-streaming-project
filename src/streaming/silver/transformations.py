from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp


def clean_bronze_data(df: DataFrame) -> DataFrame:
    """
    Clean the raw DataFrame by removing records with any null values.
    Also adds 'processed_at' timestamp and 'date' for partitioning.

    Args:
        df (DataFrame): Input DataFrame with raw data from bronze layer

    Returns:
        DataFrame: Cleaned DataFrame ready for silver layer storage
    """
    # Parse last_updated to timestamp to save as date for partitioning
    df = df.withColumn(
        "last_updated_ts",
        to_timestamp(col("last_updated"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
    )
    
    # Drop any rows that contain null values in any column
    cleaned_df = (
        df.dropna()
        .withColumn("processed_at", current_timestamp())
        .withColumn("date", to_date(col("last_updated_ts")))
        .drop("last_updated_ts")  # Drop temporary column
    )

    return cleaned_df