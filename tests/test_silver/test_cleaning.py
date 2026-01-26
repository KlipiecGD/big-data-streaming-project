import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, TimestampType
from dateutil import parser as date_parser

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
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark_session
    spark_session.stop()


def count_null_fields(record: dict) -> int:
    """Count null fields in a record."""
    return sum(1 for v in record.values() if v is None)


def extract_date_from_timestamp(timestamp_str: str) -> date | None:
    """Extract date from ISO timestamp string."""
    if timestamp_str is None:
        return None
    return date_parser.isoparse(timestamp_str).date()


class TestSilverCleaning:
    """Test suite for silver layer data cleaning transformations."""

    def test_valid_records_retained(self, spark: SparkSession):
        """Test that all valid records (no nulls) are retained."""
        assert all(count_null_fields(r) == 0 for r in VALID_BRONZE_RECORDS), (
            "VALID_BRONZE_RECORDS must contain only records with no nulls"
        )
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        expected_count = sum(1 for r in VALID_BRONZE_RECORDS if count_null_fields(r) == 0)
        assert len(result) == expected_count, "Not all valid records were retained."

    def test_records_with_nulls_filtered(self, spark: SparkSession):
        """Test that records with any null values are filtered out."""
        # Validate test data assumption
        assert all(count_null_fields(r) > 0 for r in BRONZE_RECORDS_WITH_NULLS), (
            "BRONZE_RECORDS_WITH_NULLS must contain only records with at least one null"
        )
        
        df = spark.createDataFrame(BRONZE_RECORDS_WITH_NULLS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        # All should be filtered out
        expected_count = 0
        assert len(result) == expected_count, "Records with nulls were not filtered out."

    def test_mixed_records_filtered_correctly(self, spark: SparkSession):
        """Test correct filtering on mixed valid/invalid records."""
        df = spark.createDataFrame(MIXED_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        # Count valid records
        expected_count = sum(1 for r in MIXED_BRONZE_RECORDS if count_null_fields(r) == 0)
        assert len(result) == expected_count, "Incorrect number of records after filtering."

        # Verify only valid IDs are present
        valid_ids = {r["id"] for r in MIXED_BRONZE_RECORDS if count_null_fields(r) == 0}
        result_ids = {row["id"] for row in result}
        assert result_ids == valid_ids, "Filtered records do not match expected valid records."

    def test_empty_input(self, spark: SparkSession):
        """Test handling of empty input."""
        assert len(EMPTY_BRONZE_RECORDS) == 0, "EMPTY_BRONZE_RECORDS must be empty for this test."
        df = spark.createDataFrame(EMPTY_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        assert len(result) == 0, "Empty input should result in empty output."
        assert "processed_at" in cleaned_df.columns, "processed_at column missing in output."
        assert "date" in cleaned_df.columns, "date column missing in output."

    def test_new_columns_added(self, spark: SparkSession):
        """Test that processed_at and date columns are added."""
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        for row in result:
            assert row["processed_at"] is not None, "processed_at should not be None"
            assert isinstance(row["processed_at"], datetime), "processed_at should be a datetime"
            assert row["date"] is not None, "date should not be None"
            assert hasattr(row["date"], "year"), "date should have a year attribute"

    def test_date_extracted_from_last_updated(self, spark: SparkSession):
        """Test that date is correctly extracted from last_updated."""
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        result_by_id = {row["id"]: row for row in result}
        
        for source in VALID_BRONZE_RECORDS:
            if count_null_fields(source) == 0:
                result_row = result_by_id[source["id"]]
                expected_date = extract_date_from_timestamp(source["last_updated"])
                assert result_row["date"] == expected_date, "Expected date does not match extracted date."

    def test_no_data_loss(self, spark: SparkSession):
        """Test that all bronze columns are preserved."""
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)

        bronze_cols = set(CRYPTO_BRONZE_SCHEMA.fieldNames())
        silver_cols = set(cleaned_df.columns)
        
        assert bronze_cols.issubset(silver_cols), "Some bronze columns are missing in silver output."

    def test_output_has_no_nulls(self, spark: SparkSession):
        """Test that cleaned output contains no null values."""
        df = spark.createDataFrame(MIXED_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        for row in result:
            for field in CRYPTO_BRONZE_SCHEMA.fieldNames():
                assert row[field] is not None