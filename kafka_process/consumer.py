import json
import pyspark
import os
import sys
from spark_process.spark_transform import (
    RegionTransform,
    ReplaceTransform,
    RemovePostcodeSectionTransform,
)
from dotenv import load_dotenv
load_dotenv(override=True)

base_dir = os.getenv('AIRFLOW_HOME')
spark_packages = os.getenv("SPARK_PACKAGES")
kafka_config_file = os.getenv('KAFKA_CONFIG_FILE')
schema_config_file = os.getenv('SCHEMA_CONFIG_FILE')
databricks_config_file = os.getenv('DATABRICKS_CONFIG_FILE')
delta_table_path = os.getenv('DELTA_TABLE_PATH')
checkpoints_path = os.getenv('CHECKPOINTS_PATH')
spark_config = os.getenv('SPARK_CONFIG')

from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def create_spark_session():

    builder = (
        pyspark.sql.SparkSession.builder.appName("KafkaConsumerAndTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.orc.overwrite.output.file", "true") 
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[spark_packages]
    ).getOrCreate()

    # spark.sparkContext.setLogLevel("INFO")  # Set log level to INFO
    return spark


def load_kafka_config(config_path=kafka_config_file):
    config_path = os.path.join(base_dir, config_path)
    with open(config_path, "r") as config_file:
        return json.load(config_file)


def load_schema_from_config(config_path=schema_config_file):
    config_path = os.path.join(base_dir, config_path)
    with open(config_path, "r") as config_file:
        config = json.load(config_file)

    fields = []
    for field in config["fields"]:
        field_name = field["name"]
        field_type = map_type(field["type"])
        nullable = field["nullable"]
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
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.get("broker"))
        .option("subscribe", kafka_config.get("topic"))
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_kafka_messages(spark, df_data, schema):

    df_data = (
        df_data.selectExpr("CAST(value AS STRING) as json_data")
        .select(from_json(col("json_data"), schema).alias("json_data"))
        .select("json_data.*")
        .filter("PatientId IS NOT NULL AND PatientName IS NOT NULL")
    )

    return df_data

# Load spark config and transform the data
def load_transforms(config_path=spark_config):
    config_path = os.path.join(base_dir, config_path)
    with open(config_path) as data_file:
        config = json.load(data_file)

    transforms = []
    for transform_config in config["transforms"]:
        if transform_config["name"] == "Region":
            transforms.append(RegionTransform(transform_config))
        elif transform_config["name"] == "PatientName":
            transforms.append(ReplaceTransform(transform_config))
        elif transform_config["name"] == "PostCode":
            transforms.append(RemovePostcodeSectionTransform(transform_config))

    return transforms


def apply_transformations(df_data, transforms):
    for transform in transforms:
        df_data = transform.modify_or_create(df_data)

    return df_data


def load_databricks_config(config_path=databricks_config_file):
    with open(config_path, "r") as config_obj:
        return json.load(config_obj)


def write_to_delta(df_data, target="local"):
   
    if target == "local":
        output_path = os.path.join(base_dir, delta_table_path)  # e.g., data/delta/patients_transformed
        checkpoint_path = os.path.join(base_dir, checkpoints_path)  # e.g., data/checkpoints

    # Load Databricks config only if needed
    elif target == "databricks":
        databricks_config_path = os.path.join(base_dir, databricks_config_file)
        databricks_config = load_databricks_config(databricks_config_path)
        output_path = databricks_config["dbfs_path"]
        checkpoint_path = output_path + "_checkpoints"

    else:
        raise ValueError("Invalid target specified. Use 'local' or 'databricks'.")

    df_data.writeStream.format("delta").outputMode("append").option(
        "checkpointLocation", checkpoint_path
    ).start(output_path)


def process():

    spark = create_spark_session()
    kafka_config = load_kafka_config()
    schema = load_schema_from_config()

    df_data = read_kafka_stream(spark, kafka_config)
    df_data = parse_kafka_messages(spark, df_data, schema)

    transforms = load_transforms()
    df_data = apply_transformations(df_data, transforms)

    # Choose where to write — "local" or "databricks"
    write_to_delta(df_data)

    print("Kafka consumer is processing data and writing to Delta Table.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    process()