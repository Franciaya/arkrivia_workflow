import json
import pytest
from pyspark.sql import SparkSession
from spark_process.spark_transform import (
    RegionTransform,
    ReplaceTransform,
    RemovePostcodeSectionTransform,
)


# Load test data dynamically from the JSON file
@pytest.fixture(scope="session")
def test_data():
    with open("tests/config/test_data.json", "r") as json_file:
        return json.load(json_file)


@pytest.fixture(scope="session")
def test_transform():
    with open("tests/config/test_transform_config.json", "r") as json_file:
        return json.load(json_file)


@pytest.fixture(scope="session")
def spark_session():
    # Create a Spark session for testing
    spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
    yield spark
    spark.stop()


def test_region_transform(spark_session, test_data, test_transform):
    df_data = spark_session.createDataFrame(test_data)
    for transform_config in test_transform["transforms"]:
        if transform_config["name"] == "Region":
            transform = RegionTransform(transform_config)
    df_transformed = transform.modify_or_create(df_data)
    assert "Region" in df_transformed.columns


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
