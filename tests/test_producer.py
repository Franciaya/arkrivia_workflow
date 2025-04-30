import json
import pytest
import os
from unittest.mock import patch, MagicMock
from dotenv import load_dotenv
from kafka_process.producer import (
    load_patient_data,
    send_patient_data,
) 

load_dotenv(override=True)

kafka_test_config = os.getenv("KAFKA_TEST_CONFIG")

# Load Kafka config and test data from actual files for the test
with open(kafka_test_config , "r") as data_file:
    kafka_config = json.load(data_file)

KAFKA_BROKER = kafka_config.get("broker")
KAFKA_TOPIC = kafka_config.get("topic")
DATA_FILE = kafka_config.get("data_file")


@pytest.fixture
def test_data():
    # Load the test patient data from the JSON file dynamically
    with open(DATA_FILE, "r") as data_file:
        return json.load(data_file)


@patch("kafka_process.producer.KafkaProducer")
@patch("kafka_process.producer.load_patient_data")
@patch("kafka_process.producer.json.load")
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
    calls = [MagicMock().send(kafka_topic, value=record) for record in test_data]
    mock_producer_instance.send.assert_has_calls(calls, any_order=True)

    # Ensure flush and close were called
    mock_producer_instance.flush.assert_called_once()
    mock_producer_instance.close.assert_called_once()


def test_load_patient_data():
    # Load actual test data from file
    result = load_patient_data(DATA_FILE)

    with open(DATA_FILE, "r") as fs:
        expected_data = json.load(fs)

    assert result == expected_data
