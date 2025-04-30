import json
import pytest
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from spark_process.spark_transform import (
    RegionTransform,
    ReplaceTransform,
    RemovePostcodeSectionTransform,
)

load_dotenv(override=True)
tests_data = os.getenv("TESTS_DATA")

# Load test data dynamically from the JSON file
@pytest.fixture
def test_data():
    with open(tests_data, "r") as json_file:
        return json.load(json_file)


@pytest.fixture(scope="class")
def spark_session():
    # Create a Spark session for testing
    spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
    yield spark
    spark.stop()


def test_region_transform(spark_session, test_data):
    df_data = spark_session.createDataFrame(test_data)
    transform = RegionTransform(
        {
            "mapping": {
                "North England": ["Leeds", "Manchester"],
                "Mid-West England": ["Birmingham"],
            }
        }
    )
    df_transformed = transform.modify_or_create(df_data)
    assert "region_upper" in df_transformed.columns


def test_replace_transform(spark_session, test_data):
    df_data = spark_session.createDataFrame(test_data)
    transform = ReplaceTransform({"name": "PatientName", "new_value": "XXXXX"})
    df_transformed = transform.modify_or_create(df_data)
    results = [row.PatientName for row in df_transformed.collect()]
    assert all(name == "XXXXX" for name in results)


def test_remove_postcode_section_transform(spark_session, test_data):
    df_data = spark_session.createDataFrame(test_data)
    transform = RemovePostcodeSectionTransform({"name": "PostCode"})
    df_transformed = transform.modify_or_create(df_data)

    results = [row["PostCode"] for row in df_transformed.collect()]
    assert all(" " not in val for val in results)
