from pyspark.sql import SparkSession
import json
from spark_process.spark_transform import RegionTransform, ReplaceTransform, RemovePostcodeSectionTransform

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaPatientConsumerAndTransform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def load_kafka_config(config_path="config/kafka_config.json"):
    with open(config_path, "r") as config_file:
        return json.load(config_file)

def read_kafka_stream(spark, kafka_config):
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config.get("broker")) \
        .option("subscribe", kafka_config.get("topic")) \
        .load()

def parse_kafka_messages(spark, df):  # Added spark as argument
    df = df.selectExpr("CAST(value AS STRING) as json_data")
    return spark.read.json(df.rdd.map(lambda x: x.json_data))  # Use spark passed as argument

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

def apply_transformations(df, transforms):
    for transform in transforms:
        df = transform.create_new_col(df)
    return df

def write_to_delta(df):
    df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "data/checkpoints") \
        .start("data/delta/patients_transformed")

def main():
    spark = create_spark_session()
    kafka_config = load_kafka_config()
    df_data = read_kafka_stream(spark, kafka_config)
    df_data = parse_kafka_messages(spark, df_data)  # Pass spark to this function
    transforms = load_transforms()
    df_data = apply_transformations(df_data, transforms)
    write_to_delta(df_data)
    print("Kafka consumer is processing data, transforming, and writing directly to Delta Table.")

if __name__ == "__main__":
    main()
