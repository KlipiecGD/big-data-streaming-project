import pytest
from pyspark.sql import SparkSession

from src.streaming.silver.transformations import clean_bronze_data
from src.schemas.crypto_schema import CRYPTO_BRONZE_SCHEMA
from tests.data.bronze_data import (
    VALID_BRONZE_RECORDS,
    BRONZE_RECORDS_WITH_NULLS,
    MIXED_BRONZE_RECORDS,
    EMPTY_BRONZE_RECORDS,
)
from tests.data.expected_fields import BRONZE_EXPECTED_FIELDS, SILVER_EXPECTED_FIELDS


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


class TestSilverCleaning:
    """Test suite for silver layer data cleaning transformations."""

    def test_all_expected_columns_present(self, spark: SparkSession):
        """Test that all expected silver columns are present in output."""
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)

        missing_cols = set(SILVER_EXPECTED_FIELDS) - set(cleaned_df.columns)
        assert not missing_cols, f"Missing columns in output: {missing_cols}"

    def test_null_records_are_filtered(self, spark: SparkSession):
        """Test that records with any null values are filtered out."""
        # Validate test data assumption
        assert all(any(v is None for v in r.values()) for r in BRONZE_RECORDS_WITH_NULLS), (
            "BRONZE_RECORDS_WITH_NULLS must have all records with at least one null value"
        )
        
        df = spark.createDataFrame(BRONZE_RECORDS_WITH_NULLS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        # All records with nulls should be filtered
        assert len(result) == 0, "Records with nulls should be completely filtered out."

    def test_valid_records_pass_through(self, spark: SparkSession):
        """Test that valid records without nulls pass through successfully."""
        # Validate test data assumption
        assert all(all(v is not None for v in r.values()) for r in VALID_BRONZE_RECORDS), (
            "VALID_BRONZE_RECORDS must have all records with no null values"
        )
        
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        assert len(result) > 0, "Valid records should pass through cleaning."
        assert len(result) == len(VALID_BRONZE_RECORDS), "All valid records should be retained."

    def test_mixed_data_filters_correctly(self, spark: SparkSession):
        """Test that mixed valid/invalid records are filtered correctly."""
        # Validate test data assumption
        valid_count = sum(1 for r in MIXED_BRONZE_RECORDS if all(v is not None for v in r.values()))
        invalid_count = sum(1 for r in MIXED_BRONZE_RECORDS if any(v is None for v in r.values()))
        
        assert valid_count > 0 and invalid_count > 0, (
            "MIXED_BRONZE_RECORDS must contain both valid and invalid records"
        )
        
        df = spark.createDataFrame(MIXED_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        # Only records without nulls should remain
        assert len(result) == valid_count, "Only records without nulls should remain."

    def test_empty_input_produces_empty_output(self, spark: SparkSession):
        """Test that empty input produces empty output with correct schema."""
        # Validate test data assumption
        assert len(EMPTY_BRONZE_RECORDS) == 0, (
            "EMPTY_BRONZE_RECORDS must be an empty list"
        )
        
        df = spark.createDataFrame(EMPTY_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        assert len(result) == 0, "Empty input should produce empty output."
        # Verify schema still contains expected columns
        missing_cols = set(SILVER_EXPECTED_FIELDS) - set(cleaned_df.columns)
        assert not missing_cols, f"Schema should still have all columns: {missing_cols}"

    def test_processed_at_is_added(self, spark: SparkSession):
        """Test that processed_at timestamp is added to records."""
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        assert "processed_at" in cleaned_df.columns, "processed_at column should be added."
        for row in result:
            assert row["processed_at"] is not None, "processed_at should not be None."

    def test_date_is_extracted(self, spark: SparkSession):
        """Test that date is extracted from last_updated timestamp."""
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        assert "date" in cleaned_df.columns, "date column should be added."
        for row in result:
            assert row["date"] is not None, "date should not be None."

    def test_output_has_no_nulls(self, spark: SparkSession):
        """Test that cleaned output contains no null values in any field."""
        df = spark.createDataFrame(MIXED_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)
        result = cleaned_df.collect()

        for row in result:
            for field in BRONZE_EXPECTED_FIELDS:
                assert row[field] is not None, f"Field {field} should not be None in cleaned output."

    def test_bronze_columns_preserved(self, spark: SparkSession):
        """Test that all original bronze columns are preserved in silver."""
        df = spark.createDataFrame(VALID_BRONZE_RECORDS, schema=CRYPTO_BRONZE_SCHEMA)
        cleaned_df = clean_bronze_data(df)

        missing_cols = set(BRONZE_EXPECTED_FIELDS) - set(cleaned_df.columns)
        assert not missing_cols, f"Original bronze columns should be preserved: {missing_cols}"