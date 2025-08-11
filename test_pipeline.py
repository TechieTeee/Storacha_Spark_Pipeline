import os
import pytest
from pyspark.sql import SparkSession
from spark_storacha_pipeline import validate_input_file, create_spark_session

@pytest.fixture
def temp_file(tmpdir):
    file = tmpdir.join("input.txt")
    file.write("hello world")
    return str(file)

def test_validate_input_file_exists(temp_file):
    """Tests that validate_input_file returns True for an existing file."""
    assert validate_input_file(temp_file) is True

def test_validate_input_file_not_exists():
    """Tests that validate_input_file returns False for a non-existing file."""
    assert validate_input_file("non_existent_file.txt") is False

def test_validate_input_file_empty(tmpdir):
    """Tests that validate_input_file returns True for an empty file."""
    file = tmpdir.join("empty.txt")
    file.write("")
    assert validate_input_file(str(file)) is True

def test_create_spark_session():
    """Tests that create_spark_session returns a SparkSession object."""
    spark = create_spark_session("test_app")
    assert isinstance(spark, SparkSession)
    spark.stop()
