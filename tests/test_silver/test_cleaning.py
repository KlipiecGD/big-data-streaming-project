import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType

from src.streaming.silver.transformations import clean_bronze_data
from src.schemas.crypto_schema import CRYPTO_BRONZE_SCHEMA
from tests.data.bronze_data import (
    VALID_BRONZE_RECORDS,
    BRONZE_RECORDS_WITH_NULLS,
    MIXED_BRONZE_RECORDS,
    EMPTY_BRONZE_RECORDS,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark_session = (
        SparkSession.builder.appName("TestSilverTransformations")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark_session
    spark_session.stop()


class TestSilverCleaning:
    """Test suite for silver layer data cleaning transformations."""

    def test_clean_valid_bronze_data(self, spark: SparkSession):
        """Test that valid bronze data is properly cleaned and enriched."""
        # Create DataFrame from valid bronze records
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)

        # Apply cleaning transformation
        cleaned_df = clean_bronze_data(df)

        # Collect results
        result = cleaned_df.collect()

        # Assertions
        assert len(result) == 3, "All valid records should be retained"

        # Check that processed_at column exists and is timestamp type
        assert "processed_at" in cleaned_df.columns, (
            "processed_at column should be added"
        )
        assert isinstance(cleaned_df.schema["processed_at"].dataType, TimestampType), (
            "processed_at column should be of TimestampType"
        )

        # Check that date column exists and is date type
        assert "date" in cleaned_df.columns, "date column should be added"
        assert isinstance(cleaned_df.schema["date"].dataType, DateType), (
            "date column should be of DateType"
        )

        # Verify all records have processed_at and date values
        for row in result:
            assert row["processed_at"] is not None, "processed_at should not be null"
            assert row["date"] is not None, "date should not be null"

        # Verify date is extracted from last_updated (all records have same last_updated date)
        for row in result:
            assert row["date"] == date(2026, 1, 15), (
                "date should be extracted from last_updated timestamp"
            )

        # Verify original data integrity
        assert result[0]["id"] == "bitcoin", "First record should be bitcoin"
        assert result[0]["current_price"] == 45000.50, "Bitcoin price should match"
        assert result[1]["id"] == "ethereum", "Second record should be ethereum"
        assert result[1]["current_price"] == 2500.75, "Ethereum price should match"

    def test_filter_out_null_records(self, spark: SparkSession):
        """Test that records with null values are filtered out."""
        # Create DataFrame from bronze records with nulls
        df = spark.createDataFrame(
            BRONZE_RECORDS_WITH_NULLS, schema=CRYPTO_BRONZE_SCHEMA
        )

        # Apply cleaning transformation
        cleaned_df = clean_bronze_data(df)

        # Collect results
        result = cleaned_df.collect()

        # Assertions
        assert len(result) == 0, "All records with nulls should be filtered out"

    def test_mixed_valid_and_invalid_records(self, spark: SparkSession):
        """Test filtering on mixed dataset with valid and invalid records."""
        # Create DataFrame from mixed bronze records
        df = spark.createDataFrame(MIXED_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)

        # Apply cleaning transformation
        cleaned_df = clean_bronze_data(df)

        # Collect results
        result = cleaned_df.collect()

        # Assertions
        assert len(result) == 2, "Only valid records should be retained"

        # Verify correct records were kept
        ids = [row["id"] for row in result]
        assert "bitcoin" in ids, "Bitcoin should be retained"
        assert "cardano" in ids, "Cardano should be retained"
        assert "ethereum" not in ids, "Had null current_price"
        assert "polkadot" not in ids, "Had null last_updated"

    def test_empty_input_dataframe(self, spark: SparkSession):
        """Test handling of empty input DataFrame."""
        # Create empty DataFrame with schema
        df = spark.createDataFrame(EMPTY_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)

        # Apply cleaning transformation
        cleaned_df = clean_bronze_data(df)

        # Collect results
        result = cleaned_df.collect()

        # Assertions
        assert len(result) == 0, "Empty input should produce empty output"

        # Verify schema is still correct with added columns
        assert "processed_at" in cleaned_df.columns, (
            "processed_at column should be added"
        )
        assert "date" in cleaned_df.columns, "date column should be added"

    def test_processed_at_timestamp_format(self, spark: SparkSession):
        """Test that processed_at timestamp is in correct format."""
        # Create DataFrame from valid bronze records
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)

        # Apply cleaning transformation
        cleaned_df = clean_bronze_data(df)

        # Collect results
        result = cleaned_df.collect()

        # Check processed_at is a valid datetime object
        for row in result:
            assert isinstance(row["processed_at"], datetime), (
                "processed_at should be a datetime object"
            )
            # Verify it's a recent timestamp (should be close to now)
            assert row["processed_at"].year >= 2026, (
                "processed_at year should be 2026 or later"
            )

    def test_date_partition_column_from_last_updated(self, spark: SparkSession):
        """Test that date column is extracted from last_updated timestamp."""
        # Create DataFrame from valid bronze records
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)

        # Apply cleaning transformation
        cleaned_df = clean_bronze_data(df)

        # Collect results
        result = cleaned_df.collect()

        # Check date format and that it matches last_updated
        for row in result:
            # Date should be a date object (not datetime)
            assert hasattr(row["date"], "year"), "date should have year attribute"
            assert hasattr(row["date"], "month"), "date should have month attribute"
            assert hasattr(row["date"], "day"), "date should have day attribute"
            
            # Verify date matches the date from last_updated (2026-01-15)
            assert row["date"] == date(2026, 1, 15), (
                "date should match the date portion of last_updated"
            )

    def test_no_data_loss_for_valid_records(self, spark: SparkSession):
        """Test that all columns from bronze are preserved in silver."""
        # Create DataFrame from valid bronze records
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)

        # Apply cleaning transformation
        cleaned_df = clean_bronze_data(df)

        # Verify all original columns are present
        bronze_columns = set(CRYPTO_BRONZE_SCHEMA.fieldNames())
        silver_columns = set(cleaned_df.columns)

        assert bronze_columns.issubset(silver_columns), (
            "All bronze columns should be present in silver layer"
        )

        # Verify new columns are added
        assert "processed_at" in silver_columns, "processed_at column should be added"
        assert "date" in silver_columns, "date column should be added"