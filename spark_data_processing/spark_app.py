from pyspark.sql import SparkSession
from spark_data_processing.spark_transform import RegionTransform, ReplaceTransform, RemovePostcodeSectionTransform

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransformPatientData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read raw data from the Delta table (written by Kafka consumer)
df = spark.read.format("delta").load("data/delta/patients")

# Load JSON config file for transformations
with open('config.json') as f:
    config = json.load(f)

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
    df = transform.apply(df)

# Write transformed data back to Delta table
df.write.format("delta").mode("overwrite").save("data/delta/transformed_patients")

print("Data successfully transformed and written to Delta Table")
