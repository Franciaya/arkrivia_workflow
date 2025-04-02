import json
import pytest
import spark_process.spark_transform as transform  

@pytest.fixture
def test_data():
    #Load test data from a JSON file.
    with open("tests/test_data.json", "r") as data_file:
        return json.load(data_file)

def test_remove_last_postcode_section(test_data):
    for record in test_data:
        original_postcode = record["PostCode"]
        expected_postcode = original_postcode.split()[0]  # Extract first section of postcode

        transformed_record = transform.remove_last_postcode_section(record)
        assert transformed_record["PostCode"] == expected_postcode

def test_anonymize_name(test_data):
    for record in test_data:
        transformed_record = transform.anonymize_name(record)
        assert transformed_record["PatientName"] == "XXXXX"