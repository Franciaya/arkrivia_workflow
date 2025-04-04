from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:3.1.0," \
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()


# Path to your Delta table
delta_table_path = "data/delta/patients_transformed"

# Read the Delta table
df = spark.read.format("delta").load(delta_table_path)

# Show some data
df.show()