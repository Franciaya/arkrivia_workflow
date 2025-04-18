import json
import pyspark
import os
from delta import *
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from spark_process.spark_transform import RegionTransform, ReplaceTransform, RemovePostcodeSectionTransform


load_dotenv()

spark_packages = os.getenv("SPARK_PACKAGES")

def create_spark_session():

    builder = pyspark.sql.SparkSession.builder.appName("KafkaConsumerAndTransform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder, extra_packages=[spark_packages]).getOrCreate()

    # spark.sparkContext.setLogLevel("INFO")  # Set log level to INFO
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
        .option("startingOffsets", "earliest") \
        .load()

def parse_kafka_messages(spark, df_data, schema):  # Pass the schema to this function

    df_data = df_data.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), schema).alias("json_data")) \
        .select("json_data.*") \
        .filter("PatientId IS NOT NULL AND PatientName IS NOT NULL")

    return df_data

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

def load_databricks_config(path="config/databricks_config.json"):
    with open(path, "r") as file:
        return json.load(file)
    

def write_to_delta(df_data, target="local", config_path="config/databricks_config.json"):

    if target == "local":
        output_path = "data/delta/patients_transformed"
        checkpoint_path = "data/checkpoints"
    elif target == "databricks":
        databricks_config = load_databricks_config(config_path)
        output_path = databricks_config["dbfs_path"]
        checkpoint_path = output_path + "_checkpoints"
    else:
        raise ValueError("Invalid target specified. Use 'local' or 'databricks'.")

    df_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(output_path)


def main():

    spark = create_spark_session()
    kafka_config = load_kafka_config()
    schema = load_schema_from_config("config/schema_config.json")

    df_data = read_kafka_stream(spark, kafka_config)
    df_data = parse_kafka_messages(spark, df_data, schema)

    transforms = load_transforms()
    df_data = apply_transformations(df_data, transforms)

    # Choose where to write — "local" or "databricks"
    write_to_delta(df_data)

    print("Kafka consumer is processing data and writing to Delta Table.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
