from pyspark.sql import functions as fs


# Base Transform Class
class Transform:

    def __init__(self, config):
        self.config = config
    
    def modify_or_create(self, df_data):
        raise NotImplementedError("Subclasses should implement this method.")

# Concrete Transformation Classes

class RegionTransform(Transform):
    def modify_or_create(self, df_data):
        mapping = self.config['mapping']
        return df_data.withColumn(
            "Region",
            fs.when(fs.col("City").isin(*mapping["North England"]), fs.lit("North England"))
            .when(fs.col("City").isin(*mapping["Mid-West England"]), fs.lit("Mid-West England"))
            .otherwise(fs.lit("South England & Wales"))
        )

class ReplaceTransform(Transform):
    def modify_or_create(self, df_data):
        return df_data.withColumn("PatientName", fs.lit(self.config['new_value']))


class RemovePostcodeSectionTransform(Transform):
    def modify_or_create(self, df_data):
        # Find the position of the first space in PostCode
        pos_space = fs.instr(fs.col("PostCode"), " ")

        # Use substring function to extract only the part before the first space
        return df_data.withColumn(
            "PostCode",
            fs.when(
                pos_space > 1,  # Ensure there is a space in the PostCode
                fs.col("PostCode").substr(fs.lit(1), pos_space - fs.lit(1) ) # Extract substring before the first space
            ).otherwise(fs.col("PostCode"))  # If no space, keep original
        )