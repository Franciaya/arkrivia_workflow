from pyspark.sql import functions as fs


# Base Transform Class
class Transform:

    def __init__(self, config):
        self.config = config
    
    def create_new_col(self, df_data):
        raise NotImplementedError("Subclasses should implement this method.")

# Concrete Transformation Classes

class RegionTransform(Transform):
    def create_new_col(self, df_data):
        mapping = self.config['mapping']
        return df_data.withColumn(
            "Region",
            fs.when(fs.col("City").isin(*mapping["North England"]), fs.lit("North England"))
            .when(fs.col("City").isin(*mapping["Mid-West England"]), fs.lit("Mid-West England"))
            .otherwise(fs.lit("South England & Wales"))
        )

class ReplaceTransform(Transform):
    def create_new_col(self, df_data):
        return df_data.withColumn("PatientName", fs.lit(self.config['new_value']))

class RemovePostcodeSectionTransform(Transform):
    def create_new_col(self, df_data):
        return df_data.withColumn(
            "PostCode",
            fs.when(fs.col("PostCode").contains(" "), fs.col("PostCode").substr(1, fs.col("PostCode").find(" ")-1)).otherwise(fs.col("PostCode"))
        )
