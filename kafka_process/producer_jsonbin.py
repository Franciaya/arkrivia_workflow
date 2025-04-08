import json
import os
from kafka import KafkaProducer
from ingestion_jsonio_api.ingestion_api import get_jsonbin_api

# Load Kafka config
with open("config/kafka_config.json") as data_file:
    kafka_config = json.load(data_file)

KAFKA_BROKER = kafka_config.get("broker")
KAFKA_TOPIC = kafka_config.get("topic")

def load_patient_data():
    # Load patient data from a JSONBIN Request API
    return get_jsonbin_api()

def send_patient_data():
    # Send patient data to Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    # Load generated patient data
    patient_data = load_patient_data()

    for record in patient_data:
        producer.send(KAFKA_TOPIC, value=record)
        print(f"Forwarded: {record}")

    producer.flush()
    producer.close()
    print(f"Successfully sent {len(patient_data)} records to Kafka topic: {KAFKA_TOPIC}")

if __name__ == "__main__":
    send_patient_data()
