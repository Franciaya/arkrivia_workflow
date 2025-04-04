import json
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Add the parent directory of 'my_proj' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_process.spark_transform import RegionTransform, ReplaceTransform, RemovePostcodeSectionTransform

def create_spark_session():
    spark = SparkSession.builder \
        .appName("KafkaConsumerAndTransform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:3.1.0," \
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")  # Set log level to INFO
    return spark

def load_kafka_config(config_path="config/kafka_config.json"):
    with open(config_path, "r") as config_file:
        return json.load(config_file)

def load_schema_from_config(config_path="config/schema_config.json"):
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    fields = []
    for field in config['fields']:
        field_name = field['name']
        field_type = map_type(field['type'])
        nullable = field['nullable']
        fields.append(StructField(field_name, field_type, nullable))
    
    return StructType(fields)

# Function to map the string type in the configuration to the actual PySpark types
def map_type(type_str):
    if type_str == "IntegerType":
        return IntegerType()
    elif type_str == "StringType":
        return StringType()
    else:
        raise ValueError(f"Unsupported type: {type_str}")

def read_kafka_stream(spark, kafka_config):
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config.get("broker")) \
        .option("subscribe", kafka_config.get("topic")) \
        .load()

def parse_kafka_messages(spark, df_data, schema):  # Pass the schema to this function
    return df_data.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), schema).alias("json_data")) \
        .select("json_data.*")

def load_transforms(config_path='config/spark_config.json'):
    with open(config_path) as data_file:
        config = json.load(data_file)
    
    transforms = []
    for transform_config in config['transforms']:
        if transform_config['name'] == 'Region':
            transforms.append(RegionTransform(transform_config))
        elif transform_config['name'] == 'PatientName':
            transforms.append(ReplaceTransform(transform_config))
        elif transform_config['name'] == 'PostCode':
            transforms.append(RemovePostcodeSectionTransform(transform_config))
    
    return transforms

def apply_transformations(df_data, transforms):
    for transform in transforms:
        df_data = transform.modify_or_create(df_data)
    return df_data

def write_to_delta(df_data):
    df_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "data/checkpoints") \
        .start("data/delta/patients_transformed") 

def main():
    spark = create_spark_session()
    
    # Load the Kafka configuration and the schema
    kafka_config = load_kafka_config()
    schema = load_schema_from_config("config/schema_config.json")  # Load schema dynamically
    
    # Read Kafka stream
    df_data = read_kafka_stream(spark, kafka_config)
    
    # Parse the Kafka messages using the schema
    df_data = parse_kafka_messages(spark, df_data, schema)  # Pass the schema here
    
    # Load and apply transformations
    transforms = load_transforms()
    df_data = apply_transformations(df_data, transforms)
    
    # Write data to Delta Lake
    write_to_delta(df_data)
    
    print("Kafka consumer is processing data, transforming, and writing directly to Delta Table.")

if __name__ == "__main__":
    main()
