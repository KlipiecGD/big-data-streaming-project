from typing import Callable, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def validate_schema_match(
    df: DataFrame, 
    expected_schema: StructType, 
    context: str = "DataFrame"
) -> None:
    """
    Validate that DataFrame schema matches expected schema exactly.
    
    Args:
        df (DataFrame): DataFrame to validate
        expected_schema (StructType): Expected StructType schema
        context (str): Description for error messages (e.g., "Input DataFrame", "Output DataFrame")

    Raises:
        AssertionError: If schemas don't match
    """
    actual_fields = {field.name: field for field in df.schema.fields}
    expected_fields = {field.name: field for field in expected_schema.fields}
    
    # Check for missing fields
    missing_fields = set(expected_fields.keys()) - set(actual_fields.keys())
    assert not missing_fields, (
        f"{context}: Missing required fields: {sorted(missing_fields)}"
    )
    
    # Check for extra fields
    extra_fields = set(actual_fields.keys()) - set(expected_fields.keys())
    assert not extra_fields, (
        f"{context}: Unexpected extra fields: {sorted(extra_fields)}"
    )
    
    # Check field types and nullability
    mismatches = []
    for field_name in expected_fields:
        expected_field = expected_fields[field_name]
        actual_field = actual_fields[field_name]
        
        if actual_field.dataType != expected_field.dataType:
            mismatches.append(
                f"  - '{field_name}': expected type {expected_field.dataType}, "
                f"got {actual_field.dataType}"
            )
        
        if actual_field.nullable != expected_field.nullable:
            mismatches.append(
                f"  - '{field_name}': expected nullable={expected_field.nullable}, "
                f"got nullable={actual_field.nullable}"
            )
    
    assert not mismatches, (
        f"{context}: Schema validation failed:\n" + "\n".join(mismatches)
    )


def validate_columns_exist(
    df: DataFrame, 
    column_names: list[str],
    context: str = "DataFrame"
) -> None:
    """
    Validate that specific columns exist in DataFrame.
    
    Args:
        df (DataFrame): DataFrame to validate
        column_names (list[str]): List of required column names
        context (str): Description for error messages

    Raises:
        AssertionError: If any columns are missing
    """
    actual_columns = set(df.columns)
    missing = [col for col in column_names if col not in actual_columns]
    
    assert not missing, (
        f"{context}: Required columns not found: {sorted(missing)}"
    )


def validate_column_type(
    df: DataFrame,
    column_name: str,
    expected_type,
    context: str = "DataFrame"
) -> None:
    """
    Validate that a column has the expected data type.
    
    Args:
        df (DataFrame): DataFrame to validate
        column_name (str): Column name to check
        expected_type (DataType): Expected PySpark data type
        context (str): Description for error messages

    Raises:
        AssertionError: If column type doesn't match
    """
    assert column_name in df.columns, (
        f"{context}: Column '{column_name}' not found"
    )
    
    actual_type = df.schema[column_name].dataType
    assert actual_type == expected_type, (
        f"{context}: Column '{column_name}' type mismatch - "
        f"expected {expected_type}, got {actual_type}"
    )


def validate_no_nulls(
    df: DataFrame,
    column_names: list[str] | None = None,
    context: str = "DataFrame"
) -> None:
    """
    Validate that specified columns contain no null values.
    
    Args:
        df: DataFrame to validate
        column_names: List of columns to check (None = check all)
        context: Description for error messages
    
    Raises:
        AssertionError: If any nulls are found
    """
    columns_to_check = column_names if column_names else df.columns
    
    for col in columns_to_check:
        null_count = df.filter(df[col].isNull()).count()
        assert null_count == 0, (
            f"{context}: Column '{col}' contains {null_count} null value(s)"
        )


def validate_data_condition(
    data: list[dict],
    condition_func: Callable[[list[dict]], bool],
    condition_description: str
) -> None:
    """
    Validate that test data meets a specific condition.
    
    Args:
        data: List of record dictionaries
        condition_func: Function that returns True if data is valid
        condition_description: Human-readable description of the condition
    
    Raises:
        AssertionError: If condition is not met
    """
    assert condition_func(data), (
        f"Test data validation failed: {condition_description}"
    )


def validate_all_bronze_columns_preserved(
    df: DataFrame,
    bronze_schema: StructType,
    context: str = "DataFrame"
) -> None:
    """
    Validate that all bronze columns are preserved in the output DataFrame.
    
    Args:
        df: DataFrame to validate
        bronze_schema: Bronze layer schema
        context: Description for error messages
    
    Raises:
        AssertionError: If any bronze columns are missing
    """
    bronze_cols = set(bronze_schema.fieldNames())
    output_cols = set(df.columns)
    
    missing_cols = bronze_cols - output_cols
    assert not missing_cols, (
        f"{context}: Bronze columns missing from output: {sorted(missing_cols)}"
    )


def validate_column_values_in_set(
    df: DataFrame,
    column_name: str,
    allowed_values: set[Any],
    context: str = "DataFrame"
) -> None:
    """
    Validate that all values in a column are within an allowed set.
    
    Args:
        df: DataFrame to validate
        column_name: Column name to check
        allowed_values: Set of allowed values
        context: Description for error messages
    
    Raises:
        AssertionError: If any values are outside the allowed set
    """
    assert column_name in df.columns, (
        f"{context}: Column '{column_name}' not found"
    )
    
    distinct_values = set(row[column_name] for row in df.select(column_name).distinct().collect())
    invalid_values = distinct_values - allowed_values
    
    assert not invalid_values, (
        f"{context}: Column '{column_name}' contains invalid values: {invalid_values}. "
        f"Allowed values: {allowed_values}"
    )


def validate_numeric_range(
    df: DataFrame,
    column_name: str,
    min_value: float | None = None,
    max_value: float | None = None,
    context: str = "DataFrame"
) -> None:
    """
    Validate that numeric values in a column fall within a specified range.
    
    Args:
        df: DataFrame to validate
        column_name: Column name to check
        min_value: Minimum allowed value (inclusive)
        max_value: Maximum allowed value (inclusive)
        context: Description for error messages
    
    Raises:
        AssertionError: If any values are outside the range
    """
    assert column_name in df.columns, (
        f"{context}: Column '{column_name}' not found"
    )
    
    from pyspark.sql.functions import col, min as spark_min, max as spark_max
    
    stats = df.agg(
        spark_min(col(column_name)).alias("min"),
        spark_max(col(column_name)).alias("max")
    ).collect()[0]
    
    actual_min = stats["min"]
    actual_max = stats["max"]
    
    if min_value is not None and actual_min is not None:
        assert actual_min >= min_value, (
            f"{context}: Column '{column_name}' has value {actual_min} below minimum {min_value}"
        )
    
    if max_value is not None and actual_max is not None:
        assert actual_max <= max_value, (
            f"{context}: Column '{column_name}' has value {actual_max} above maximum {max_value}"
        )