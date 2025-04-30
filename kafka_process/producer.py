import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv(override=True)
kafka_config_file = os.getenv('KAFKA_CONFIG_FILE')

# Load Kafka config
with open(kafka_config_file) as data_file:
    kafka_config = json.load(data_file)

KAFKA_BROKER = kafka_config.get("broker")
KAFKA_TOPIC = kafka_config.get("topic")
DATA_FILE = kafka_config.get("data_file")


def load_patient_data(file_path):
    # Load patient data from a JSON file
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file {file_path} not found!")

    with open(file_path, "r") as f:
        return json.load(f)


def send_patient_data():
    # Send patient data to Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    # Load generated patient data
    patient_data = load_patient_data(DATA_FILE)

    for record in patient_data:
        producer.send(KAFKA_TOPIC, value=record)
        print(f"Forwarded: {record}")

    producer.flush()
    producer.close()
    print(
        f"Successfully sent {len(patient_data)} records to Kafka topic: {KAFKA_TOPIC}"
    )


if __name__ == "__main__":
    send_patient_data()
