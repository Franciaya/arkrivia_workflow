# Test parameters (hardcoded values allowed)
TEST_DATA_SIZE = 10  
TEST_DATA_PATH = "data/patient_data.json"  

# Kafka settings
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "patient_topic"

# Delta Table Path
DELTA_TABLE_PATH = "data/delta/patients"

# Sample patients for Spark transformation testing
SAMPLE_PATIENTS = [
    (1, "Alice Smith", "London", "SW1A 1AA", "Depression"),
    (2, "Bob Jones", "Manchester", "M1 1AE", "Schizophrenia"),
    (3, "Charlie Brown", "Cardiff", "CF10 1AA", "Cancer"),
]
