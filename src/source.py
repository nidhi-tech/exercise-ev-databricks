# Databricks notebook source
# MAGIC %run ./DatabricksHelpers

# COMMAND ----------

exercise_name = "batch_processing_bronze_ingest"
helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_dataframe(filepath: str) -> DataFrame:
    
    custom_schema = StructType([
        StructField("message_id", StringType(), True),
        StructField("message_type", IntegerType(), True),
        StructField("charge_point_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("write_timestamp", StringType(), True),
        StructField("body", StringType(), True),
    ])
    
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("delimiter", ",") \
        .option("escape", "\\") \
        .schema(custom_schema) \
        .load(filepath)
    return df

# COMMAND ----------

url = "https://raw.githubusercontent.com/kelseymok/charge-point-simulator-v1.6/main/out/1680355141.csv.gz"
filepath = helpers.download_to_local_dir(url)


# COMMAND ----------

df = create_dataframe(filepath)
display(df)

# COMMAND ----------


