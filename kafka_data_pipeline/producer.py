import json
import os
from kafka import KafkaProducer

# Load Kafka config
with open("config/kafka_config.json") as data_file:
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
    """Send patient data to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Load generated patient data
    patient_data = load_patient_data(DATA_FILE)

    for record in patient_data:
        producer.send(KAFKA_TOPIC, value=record)
        print(f"📤 Sent: {record}")

    producer.flush()
    producer.close()
    print(f"✅ Successfully sent {len(patient_data)} records to Kafka topic: {KAFKA_TOPIC}")

if __name__ == "__main__":
    send_patient_data()
