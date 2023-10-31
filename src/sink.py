# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Gold
# MAGIC
# MAGIC Remember our domain question, **What is the final charge time and final charge dispense for every completed transaction**? It was the exercise which required several joins and window queries. :)  We're here to do it again (the lightweight version) but with the help of the work we did in the transform. 
# MAGIC
# MAGIC Steps:
# MAGIC * Match StartTransaction Requests and Responses
# MAGIC * Join Stop Transaction Requests and StartTransaction Responses, matching on transaction_id (left join)
# MAGIC * Find the matching StartTransaction Requests (left join)
# MAGIC * Calculate the total_time (withColumn, cast, maths)
# MAGIC * Calculate total_energy (withColumn, cast)
# MAGIC * Calculate total_parking_time (explode, filter, window, groupBy)
# MAGIC * Join and Shape (left join, select) 
# MAGIC * Write to Parquet
# MAGIC

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

exercise_name = "batch_processing_gold"

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

working_directory = helpers.working_directory()
out_dir = f"{working_directory}/output/"
print(out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write to Parquet
# MAGIC In this exercise, write the DataFrame `f"{out_dir}/cdr"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

def write_to_parquet(input_df: DataFrame):
    output_directory = f"{out_dir}/cdr"
    ### YOUR CODE HERE
    mode_type: str  = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_type).\
        parquet(output_directory)

write_to_parquet(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     ))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/cdr")))

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/vijaya.durga/
# MAGIC

# COMMAND ----------


