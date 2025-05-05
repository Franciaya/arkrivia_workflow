import json
import pytest
from unittest.mock import patch, MagicMock, call
from kafka_process.producer_jsonbin import (
    load_patient_data,
    send_patient_data,
)

# Load Kafka config and test data from actual files for the test
with open("tests/config/test_kafka_config.json", "r") as data_file:
    kafka_config = json.load(data_file)

KAFKA_BROKER = kafka_config.get("broker")
KAFKA_TOPIC = kafka_config.get("topic")
DATA_FILE = kafka_config.get("data_file")


@pytest.fixture(scope="session")
def test_data():
    # Load the test patient data from the JSON file dynamically
    with open(DATA_FILE, "r") as data_file:
        return json.load(data_file)


@patch("kafka_process.producer_jsonbin.KafkaProducer")
@patch("kafka_process.producer_jsonbin.load_patient_data")
@patch("kafka_process.producer_jsonbin.json.load")
def test_send_patient_data(
    mock_json_load, mock_load_patient_data, mock_kafka_producer, test_data
):
    # Use the dynamically loaded test data
    mock_load_patient_data.return_value = test_data

    # Mock Kafka config loading (so we use actual file contents)
    mock_json_load.return_value = kafka_config

    # Mock Kafka producer instance
    mock_producer_instance = MagicMock()
    mock_kafka_producer.return_value = mock_producer_instance

    # Call the function under test
    send_patient_data()

    # Get topic from mocked Kafka config
    kafka_topic = kafka_config["topic"]

    # Check if send was called with correct data
    calls = [call(kafka_topic, value=record) for record in test_data]
    mock_producer_instance.send.assert_has_calls(calls, any_order=True)

    # Ensure flush and close were called
    mock_producer_instance.flush.assert_called_once()
    mock_producer_instance.close.assert_called_once()


@patch("kafka_process.producer_jsonbin.get_jsonbin_api")
def test_mocked_load_patient_data(mock_get_jsonbin_api, test_data):
    # Mock the API response with test data (2 records)
    mock_get_jsonbin_api.return_value = test_data

    # Call the real load_patient_data (which will call the mocked API)
    result = load_patient_data()

    # Assert that it returns the mocked data
    assert result == test_data
