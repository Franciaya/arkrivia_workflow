from pyspark.sql import SparkSession
import json
from spark_data_processing.spark_transform import RegionTransform, ReplaceTransform, RemovePostcodeSectionTransform

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaPatientConsumerAndTransform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

with open("config/kafka_config.json", "r") as config_file:
    kafka_config = json.load(config_file)

# Kafka consumer properties
KAFKA_BROKER = kafka_config.get("broker")
KAFKA_TOPIC = kafka_config.get("topic")

# Read data from Kafka topic
df_data = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# The Kafka message value is in binary, so we need to decode it to a string and then parse it as JSON
df_data = df_data.selectExpr("CAST(value AS STRING) as json_data")

# Assuming the data is in JSON format, we parse it into a DataFrame
df_data = spark.read.json(df_data.rdd.map(lambda x: x.json_data))

# Load JSON config file for transformations
with open('config/spark_config.json') as data_file:
    config = json.load(data_file)

# Create transformations based on the config
transforms = []
for transform_config in config['transforms']:
    if transform_config['name'] == 'Region':
        transforms.append(RegionTransform(transform_config))
    elif transform_config['name'] == 'PatientName':
        transforms.append(ReplaceTransform(transform_config))
    elif transform_config['name'] == 'PostCode':
        transforms.append(RemovePostcodeSectionTransform(transform_config))

# Apply all transformations to the DataFrame
for transform in transforms:
    df_data = transform.create_new_col(df_data)

# Write transformed data directly to Delta table without storing raw data
df_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "data/checkpoints") \
    .start("data/delta/patients_transformed")

print("Kafka consumer is processing data, transforming, and writing directly to Delta Table.")
