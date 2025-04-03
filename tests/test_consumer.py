import json
import pytest
from pyspark.sql import SparkSession
from spark_process.spark_transform import RegionTransform, ReplaceTransform, RemovePostcodeSectionTransform

# Load test data dynamically from the JSON file
@pytest.fixture
def test_data():
    with open("tests/test_data.json", "r") as f:
        return json.load(f)

@pytest.fixture(scope="class")
def spark_session():
    # Create a Spark session for testing
    spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
    yield spark
    spark.stop()

def test_region_transform(spark_session, test_data):
    df_data = spark_session.createDataFrame(test_data)
    transform = RegionTransform({"name": "Region", "new_col": "region_upper"})
    df_transformed = transform.modify_or_create(df_data)
    assert "region_upper" in df_transformed.columns

def test_replace_transform(spark_session, test_data):
    df_data = spark_session.createDataFrame(test_data)
    transform = ReplaceTransform({"name": "PatientName", "replace": {"Doe": "Smith"}})
    df_transformed = transform.modify_or_create(df_data)
    results = [row.name for row in df_transformed.collect()]
    assert "John Smith" in results
    assert "Jane Smith" in results

def test_remove_postcode_section_transform(spark_session, test_data):
    df_data = spark_session.createDataFrame(test_data)
    transform = RemovePostcodeSectionTransform({"name": "PostCode"})
    df_transformed = transform.modify_or_create(df_data)
    assert all(" " not in row.postcode for row in df_transformed.collect())
