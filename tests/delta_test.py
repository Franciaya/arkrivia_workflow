from pyspark.sql import SparkSession
import os
from tests.config import DELTA_TABLE_PATH

spark = SparkSession.builder \
    .appName("TestDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def test_delta_table():
    """Test if data is correctly written to Delta Table."""
    
    if os.path.exists(DELTA_TABLE_PATH):
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        assert df.count() > 0
        assert "Region" in df.columns
