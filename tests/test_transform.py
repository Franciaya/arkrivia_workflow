import json
import pytest
from pyspark.sql import Row, SparkSession
from spark_process import spark_transform


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("pytest-pyspark").master("local[*]").getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def test_df(spark):
    with open("tests/test_data.json", "r") as data_file:
        data = json.load(data_file)
        return spark.createDataFrame([Row(**record) for record in data])


def test_remove_last_postcode_section(spark):
    # Load test data from JSON
    with open("tests/test_data.json") as f:
        raw_data = json.load(f)

    # Create expected data
    expected_postcodes = [record["PostCode"].split()[0] for record in raw_data]

    # Create DataFrame from original (untransformed) data
    test_df = spark.createDataFrame([Row(**record) for record in raw_data])

    # Apply the transform
    transform_obj = spark_transform.RemovePostcodeSectionTransform(config={})
    result_df = transform_obj.modify_or_create(test_df)
    result = result_df.collect()

    # Compare actual vs expected
    for row, expected in zip(result, expected_postcodes):
        assert row["PostCode"] == expected


def test_anonymize_name(spark, test_df):
    transform_obj = spark_transform.ReplaceTransform(config={"new_value": "XXXXX"})
    result_df = transform_obj.modify_or_create(test_df)
    result = result_df.collect()

    for row in result:
        assert row["PatientName"] == "XXXXX"
