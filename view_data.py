from pyspark.sql import SparkSession
import pyspark
import os
from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip

load_dotenv()

packages = os.getenv("SPARK_PACKAGES")

builder = pyspark.sql.SparkSession.builder.appName("KafkaConsumerAndTransform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
spark = configure_spark_with_delta_pip(builder, extra_packages=[packages]).getOrCreate()


# Path to your Delta table
delta_table_path = "data/delta/patients_transformed"

# Read the Delta table
df = spark.read.format("delta").load(delta_table_path)

# Show some data
df.show()