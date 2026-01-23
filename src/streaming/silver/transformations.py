from dotenv import load_dotenv

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date

load_dotenv()
def clean_bronze_data(df: DataFrame) -> DataFrame:
    """
    Clean the raw DataFrame by removing records with null 'id', 'symbol' or 'current_price', add 'processed_at' timestamp and
    Args:
        df (DataFrame): Input DataFrame with raw data
    Returns:
        DataFrame: Cleaned DataFrame
    """
    cleaned_df = df.filter(
        col("id").isNotNull() & 
        col("symbol").isNotNull() & 
        col("current_price").isNotNull()
    ).withColumn("processed_at", current_timestamp()).withColumn("date", to_date(col("processed_at")))

    return cleaned_df