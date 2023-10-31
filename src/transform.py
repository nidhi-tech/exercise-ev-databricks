# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Processing - Silver Tier
# MAGIC
# MAGIC In the last exercise, we took our data wrote it to the Parquet format, ready for us to pick up in the Silver Tier. In this exercise, we'll take our first step towards curation and cleanup by:
# MAGIC * Unpacking strings containing json to JSON
# MAGIC * Flattening our data (unpack nested structures and bring to top level)
# MAGIC
# MAGIC We'll do this for:
# MAGIC * StartTransaction Request
# MAGIC * StartTransaction Response
# MAGIC * StopTransaction Request
# MAGIC * MeterValues Request

# COMMAND ----------

# MAGIC %run ./source

# COMMAND ----------

exercise_name = "batch_processing_silver"

# COMMAND ----------

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Bronze Layer
# MAGIC Let's read the parquet files that we created in the Bronze layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Bronze layer but for simplicity and consistent results [of this exercise], we're going to read in a Bronze output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Filter
# MAGIC In this exercise, filter for the `StartTransaction` action and the "Request" (`2`) message_type.

# COMMAND ----------

########## SOLUTION ###########

def start_transaction_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action = "StartTransaction"
    message_type = 2
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- connector_id: integer (nullable = true)
# MAGIC  |    |-- id_tag: string (nullable = true)
# MAGIC  |    |-- meter_start: integer (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- reservation_id: integer (nullable = true)
# MAGIC  ```

# COMMAND ----------

########## SOLUTION ###########

from pyspark.sql.functions import from_json, col

def start_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("connector_id", IntegerType(), True),
        StructField("id_tag", StringType(), True),
        StructField("meter_start", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("reservation_id", IntegerType(), True),
    ])
    ### YOUR CODE HERE
    new_column_name: str = "new_body"
    from_column_name: str = "body"
    ###
    return input_df.withColumn(new_column_name,from_json(from_column_name, body_schema))

    
display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- id_tag: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- reservation_id: integer (nullable = true)
# MAGIC  ```

# COMMAND ----------

############ SOLUTION ##############

def start_transaction_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    connector_id_column_name: str = "connector_id"
    meter_start_column_name: str = "meter_start"
    timestamp_column_name: str = "timestamp"
    columns_to_drop: List[str] = ["new_body", "body"]
    ###
    return input_df.\
        withColumn(connector_id_column_name, input_df.new_body.connector_id).\
        withColumn("id_tag", input_df.new_body.id_tag).\
        withColumn(meter_start_column_name, input_df.new_body.meter_start).\
        withColumn(timestamp_column_name, input_df.new_body.timestamp).\
        withColumn("reservation_id", input_df.new_body.reservation_id).\
        drop(*columns_to_drop)

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Request Cast Columns
# MAGIC Cast the `timestamp` column to [TimestampType](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.types.TimestampType.html?highlight=timestamptype#pyspark.sql.types.TimestampType) using [to_timestamp](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.to_timestamp.html?highlight=to_timestamp#pyspark.sql.functions.to_timestamp)
# MAGIC Hint: You have to import the function from "pyspark.sql.functions" first.
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- connector_id: integer (nullable = true)
# MAGIC  |-- id_tag: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)  #=> updated
# MAGIC  |-- reservation_id: integer (nullable = true)
# MAGIC ```

# COMMAND ----------

############ SOLUTION ###########

from pyspark.sql.functions import to_timestamp
def start_transaction_request_cast(input_df: DataFrame) -> DataFrame:
    ### YOU CODE HERE
    new_column_name: str = "timestamp"
    from_column_name: str = "timestamp"
    ###
    return input_df.withColumn(new_column_name, to_timestamp(col(from_column_name)))

display(df.transform(start_transaction_request_filter).transform(start_transaction_request_unpack_json).transform(start_transaction_request_flatten).transform(start_transaction_request_cast))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StartTransaction Response

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Filter
# MAGIC In this exercise, filter for the `StartTransaction` action and the "Response" (`3`) message_type.

# COMMAND ----------

############## SOLUTION ################

def start_transaction_response_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action: str = "StartTransaction"
    message_type: int = 3
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(start_transaction_response_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- id_tag_info: struct (nullable = true)
# MAGIC  |    |    |-- status: string (nullable = true)
# MAGIC  |    |    |-- parent_id_tag: string (nullable = true)
# MAGIC  |    |    |-- expiry_date: string (nullable = true)
# MAGIC ```

# COMMAND ----------

########### SOLUTION ###########

def start_transaction_response_unpack_json(input_df: DataFrame):
    id_tag_info_schema = StructType([
        StructField("status", StringType(), True),
        StructField("parent_id_tag", StringType(), True),
        StructField("expiry_date", StringType(), True),
    ])

    body_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("id_tag_info", id_tag_info_schema, True)
    ])
    ### YOUR CODE HERE
    new_column_name: str = "new_body"
    from_column_name: str = "body"
    ###
    return input_df.withColumn(new_column_name,from_json(col(from_column_name), body_schema))
    
display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StartTransaction Response Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- id_tag_info_status: string (nullable = true)
# MAGIC  |-- id_tag_info_parent_id_tag: string (nullable = true)
# MAGIC  |-- id_tag_info_expiry_date: string (nullable = true)
# MAGIC ```

# COMMAND ----------

########### SOLUTION ###########

def start_transaction_response_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    transaction_id_column_name: str = "transaction_id"
    drop_column_names: List[str] = ["new_body", "body"]
    ###
    return input_df.\
        withColumn(transaction_id_column_name, input_df.new_body.transaction_id).\
        withColumn("id_tag_info_status", input_df.new_body.id_tag_info.status).\
        withColumn("id_tag_info_parent_id_tag", input_df.new_body.id_tag_info.parent_id_tag).\
        withColumn("id_tag_info_expiry_date", input_df.new_body.id_tag_info.expiry_date).\
        drop(*drop_column_names)

display(df.transform(start_transaction_response_filter).transform(start_transaction_response_unpack_json).transform(start_transaction_response_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process StopTransaction Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Filter
# MAGIC In this exercise, filter for the `StopTransaction` action and the "Request" (`2`) message_type.

# COMMAND ----------

############ SOLUTION #############

def stop_transaction_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action: str = "StopTransaction"
    message_type: int = 2
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(stop_transaction_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- meter_stop: integer (nullable = true)
# MAGIC  |    |-- timestamp: string (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- reason: string (nullable = true)
# MAGIC  |    |-- id_tag: string (nullable = true)
# MAGIC  |    |-- transaction_data: array (nullable = true)
# MAGIC  |    |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

############### SOLUTION ################

from pyspark.sql.types import ArrayType
    
def stop_transaction_request_unpack_json(input_df: DataFrame):
    body_schema = StructType([
        StructField("meter_stop", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("reason", StringType(), True),
        StructField("id_tag", StringType(), True),
        StructField("transaction_data", ArrayType(StringType()), True)
    ])
    ### YOUR CODE HERE
    new_column_name: str = "new_body"
    from_column_name: str = "body"
    ###
    return input_df.withColumn(new_column_name,from_json(col(from_column_name), body_schema))


display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns!
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

########### SOLUTION ############

def stop_transaction_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    meter_stop_column_name: str = "meter_stop"
    timestamp_column_name: str = "timestamp"
    transaction_id_column_name: str = "transaction_id"
    drop_column_names: List[str] = ["new_body", "body"]
    ###
    return input_df.\
        withColumn(meter_stop_column_name, input_df.new_body.meter_stop).\
        withColumn(timestamp_column_name, input_df.new_body.timestamp).\
        withColumn(transaction_id_column_name, input_df.new_body.transaction_id).\
        withColumn("reason", input_df.new_body.reason).\
        withColumn("id_tag", input_df.new_body.id_tag).\
        withColumn("transaction_data", input_df.new_body.transaction_data).\
        drop(*drop_column_names)

display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: StopTransaction Request Cast Columns
# MAGIC Cast the `timestamp` column to [TimestampType](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.types.TimestampType.html?highlight=timestamptype#pyspark.sql.types.TimestampType) using [to_timestamp](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.to_timestamp.html?highlight=to_timestamp#pyspark.sql.functions.to_timestamp).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- reason: string (nullable = true)
# MAGIC  |-- id_tag: string (nullable = true)
# MAGIC  |-- transaction_data: array (nullable = true)
# MAGIC  |    |-- element: string (containsNull = true)
# MAGIC ```

# COMMAND ----------

############ SOLUTION ###########
from pyspark.sql.functions import to_timestamp

def stop_transaction_request_cast(input_df: DataFrame) -> DataFrame:
    ### YOU CODE HERE
    new_column_name: str = "timestamp"
    from_column_name: str = "timestamp"
    ###
    return input_df.withColumn(new_column_name, to_timestamp(col(from_column_name)))

display(df.transform(stop_transaction_request_filter).transform(stop_transaction_request_unpack_json).transform(stop_transaction_request_flatten).transform(stop_transaction_request_cast))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process MeterValues Request

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Filter
# MAGIC In this exercise, filter for the `MeterValues` action and the "Request" (`2`) message_type.

# COMMAND ----------

######### SOLUTION ###########

def meter_values_request_filter(input_df: DataFrame):
    ### YOUR CODE HERE
    action: str = "MeterValues"
    message_type: int = 2
    ###
    return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

display(df.transform(meter_values_request_filter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Unpack JSON
# MAGIC In this exercise, we'll unpack the `body` column containing a json string and and create a new column `new_body` containing that parsed json, using [from_json](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.from_json.html).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- body: string (nullable = true)
# MAGIC  |-- new_body: struct (nullable = true)
# MAGIC  |    |-- connector_id: integer (nullable = true)
# MAGIC  |    |-- transaction_id: integer (nullable = true)
# MAGIC  |    |-- meter_value: array (nullable = true)
# MAGIC  |    |    |-- element: struct (containsNull = true)
# MAGIC  |    |    |    |-- timestamp: string (nullable = true)
# MAGIC  |    |    |    |-- sampled_value: array (nullable = true)
# MAGIC  |    |    |    |    |-- element: struct (containsNull = true)
# MAGIC  |    |    |    |    |    |-- value: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- context: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- format: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- measurand: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- phase: string (nullable = true)
# MAGIC  |    |    |    |    |    |-- unit: string (nullable = true)
# MAGIC ```

# COMMAND ----------

############## SOLUTION ##############

def meter_values_request_unpack_json(input_df: DataFrame):
    sampled_value_schema = StructType([
        StructField("value", StringType()),
        StructField("context", StringType()),
        StructField("format", StringType()),
        StructField("measurand", StringType()),
        StructField("phase", StringType()),
        StructField("unit", StringType()),
    ])

    meter_value_schema = StructType([
        StructField("timestamp", StringType()),
        StructField("sampled_value", ArrayType(sampled_value_schema)),
    ])

    body_schema = StructType([
        StructField("connector_id", IntegerType()),
        StructField("transaction_id", IntegerType()),
        StructField("meter_value", ArrayType(meter_value_schema)),
    ])
    ### YOUR CODE HERE
    new_column_name: str = "new_body"
    from_column_name: str = "body"
    ###

    return input_df.withColumn(new_column_name, from_json(col(from_column_name), body_schema))

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: MeterValues Request Flatten
# MAGIC In this exercise, we will flatten the nested json within the `new_body` column and pull them out to their own columns, using [withColumn](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). Don't forget to [drop](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.drop.html?highlight=drop#pyspark.sql.DataFrame.drop) extra columns! You might need to use [explode](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.explode.html?highlight=explode#pyspark.sql.functions.explode) for certain nested structures. We'll also take the additional step to convert the `timestamp` column to the TimestampType using [to_timestamp](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.to_timestamp.html?highlight=to_timestamp#pyspark.sql.functions.to_timestamp).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- message_id: string (nullable = true)
# MAGIC  |-- message_type: integer (nullable = true)
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- action: string (nullable = true)
# MAGIC  |-- write_timestamp: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- timestamp: timestamp (nullable = true)
# MAGIC  |-- measurand: string (nullable = true)
# MAGIC  |-- phase: string (nullable = true)
# MAGIC  |-- value: double (nullable = true)
# MAGIC ```

# COMMAND ----------

############ SOLUTION #############

from pyspark.sql.functions import explode, to_timestamp, round
from pyspark.sql.types import DoubleType


def meter_values_request_flatten(input_df: DataFrame):
    ### YOUR CODE HERE
    selected_column_names: List[str] = ["message_id", "message_type", "charge_point_id", "action", "write_timestamp", "transaction_id", "connector_id", "timestamp", "measurand", "phase", "value"]
    ###
    return input_df. \
        select("*", explode("new_body.meter_value").alias("meter_value")). \
        select("*", explode("meter_value.sampled_value").alias("sampled_value")). \
        withColumn("timestamp", to_timestamp(col("meter_value.timestamp"))).\
        withColumn("measurand", col("sampled_value.measurand")).\
        withColumn("phase", col("sampled_value.phase")).\
        withColumn("value", round(col("sampled_value.value").cast(DoubleType()),2)).\
        withColumn("transaction_id", col("new_body.transaction_id")).\
        withColumn("connector_id", col("new_body.connector_id")).\
        select(*selected_column_names)

display(df.transform(meter_values_request_filter).transform(meter_values_request_unpack_json).transform(meter_values_request_flatten))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Parquet

# COMMAND ----------

out_dir = f"{working_directory}/output/"
print(out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Request to Parquet
# MAGIC In this exercise, write the StartTransaction Request data to `f"{out_dir}/StartTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

############ SOLUTION ##############

def write_start_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionRequest"
    ### YOUR CODE HERE
    mode_name: str = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)
    

write_start_transaction_request(df.\
    transform(start_transaction_request_filter).\
    transform(start_transaction_request_unpack_json).\
    transform(start_transaction_request_flatten).\
    transform(start_transaction_request_cast))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StartTransaction Response to Parquet
# MAGIC In this exercise, write the StartTransaction Response data to `f"{out_dir}/StartTransactionResponse"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

############ SOLUTION ##############

def write_start_transaction_response(input_df: DataFrame):
    output_directory = f"{out_dir}/StartTransactionResponse"
    ### YOUR CODE HERE
    mode_name: str = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)


write_start_transaction_response(df.\
    transform(start_transaction_response_filter).\
    transform(start_transaction_response_unpack_json).\
    transform(start_transaction_response_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StartTransactionResponse")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write StopTransaction Request to Parquet
# MAGIC In this exercise, write the StopTransaction Request data to `f"{out_dir}/StopTransactionRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

############ SOLUTION ##############

def write_stop_transaction_request(input_df: DataFrame):
    output_directory = f"{out_dir}/StopTransactionRequest"
    ### YOUR CODE HERE
    mode_name: str = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)

write_stop_transaction_request(df.\
    transform(stop_transaction_request_filter).\
    transform(stop_transaction_request_unpack_json).\
    transform(stop_transaction_request_flatten).\
    transform(stop_transaction_request_cast))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/StopTransactionRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Write MeterValues Request to Parquet
# MAGIC In this exercise, write the MeterValues Request data to `f"{out_dir}/MeterValuesRequest"` in the [parquet format](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet) using mode `overwrite`.

# COMMAND ----------

############ SOLUTION ##############

def write_meter_values_request(input_df: DataFrame):
    output_directory = f"{out_dir}/MeterValuesRequest"
    ### YOUR CODE HERE
    mode_name: str = "overwrite"
    ###
    input_df.\
        write.\
        mode(mode_name).\
        parquet(output_directory)

write_meter_values_request(df.\
    transform(meter_values_request_filter).\
    transform(meter_values_request_unpack_json).\
    transform(meter_values_request_flatten))

display(spark.createDataFrame(dbutils.fs.ls(f"{out_dir}/MeterValuesRequest")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC Congrats for finishing the Batch Processing Silver Tier exercise! We now have unpacked and flattened data for:
# MAGIC * StartTransaction Request
# MAGIC * StartTransaction Response
# MAGIC * StopTransaction Request
# MAGIC * MeterValues Request
# MAGIC
# MAGIC Hypothetically, we could have also done the same for the remaining actions (e.g. Heartbeat Request/Response, BootNotification Request/Response), but to save some time, we've only processed the actions that are relevant to the Gold layers that we'll build next (thin-slices, ftw!). You might have noticed that some of the processing steps were a bit repetitive and especially towards the end, could definitely be D.R.Y.'ed up (and would be in production code), but for the purposes of the exercise, we've gone the long route.

# COMMAND ----------

# MAGIC %md
# MAGIC # Batch Processing - Gold
# MAGIC
# MAGIC Remember our domain question, **What is the final charge time and final charge dispense for every completed transaction**? It was the exercise which required several joins and window queries. :)  We're here to do it again (the lightweight version) but with the help of the work we did in the Silver Tier. 
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
# MAGIC **NOTE:** You've already done these exercises before. We absolutely recommend bringing over your answers from that exercise to speed things along (with some minor tweaks), because you already know how to do all of that already! Of course, you're welcome to freshly rewrite your answers to test yourself!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. **Note** that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

exercise_name = "batch_processing_gold"

# COMMAND ----------



helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Silver Layer
# MAGIC Let's read the parquet files that we created in the Silver layer!
# MAGIC
# MAGIC **Note:** normally we'd use the EXACT data and location of the data that was created in the Silver layer but for simplicity and consistent results [of this exercise], we're going to read in a Silver output dataset that has been pre-prepared. Don't worry, it's the same as the output from your exercise (if all of your tests passed)!

# COMMAND ----------

meter_values_request_filepath = f"{out_dir}/MeterValuesRequest"
start_transaction_request_filepath = f"{out_dir}/StartTransactionRequest"
start_transaction_response_filepath = f"{out_dir}/StartTransactionResponse"
stop_transaction_request_filepath = f"{out_dir}/StopTransactionRequest"


# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/vijaya.durga/batch_processing_silver/output/

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df
    
start_transaction_request_df = read_parquet(start_transaction_request_filepath)
start_transaction_response_df = read_parquet(start_transaction_response_filepath)
stop_transaction_request_df = read_parquet(stop_transaction_request_filepath)
meter_values_request_df = read_parquet(meter_values_request_filepath)

display(start_transaction_request_df)
display(start_transaction_response_df)
display(stop_transaction_request_df)
display(meter_values_request_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Match StartTransaction Requests and Responses
# MAGIC In this exercise, match StartTransaction Requests and Responses using a [left join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) on `message_id`.
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC ```

# COMMAND ----------

########## SOLUTION ##########
def match_start_transaction_requests_with_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = "inner"
    ###
    return input_df.\
        join(join_df, input_df.message_id == join_df.message_id, join_type).\
        select(
            input_df.charge_point_id.alias("charge_point_id"), 
            input_df.transaction_id.alias("transaction_id"), 
            join_df.meter_start.alias("meter_start"), 
            join_df.timestamp.alias("start_timestamp")
        )
    start_transaction_response_df
display(start_transaction_response_df.transform(match_start_transaction_requests_with_responses, start_transaction_request_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join Stop Transaction Requests and StartTransaction Responses
# MAGIC In this exercise, [left join](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html) Stop Transaction Requests and the newly joined StartTransaction Request/Response DataFrame (from the previous exercise), matching on transaction_id (left join).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  ```

# COMMAND ----------

############## SOLUTION ##############
def join_with_start_transaction_responses(input_df: DataFrame, join_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = "left"
    ###
    return input_df. \
    join(join_df, input_df.transaction_id == join_df.transaction_id, join_type). \
    select(
        join_df.charge_point_id, 
        join_df.transaction_id, 
        join_df.meter_start, 
        input_df.meter_stop.alias("meter_stop"), 
        join_df.start_timestamp, 
        input_df.timestamp.alias("stop_timestamp")
    )

    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate the total_time
# MAGIC Using Pyspark functions [withColumn](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html) and [cast](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.Column.cast.html?highlight=cast#pyspark.sql.Column.cast) and little creative maths, calculate the total charging time (stop_timestamp - start_timestamp) in hours (two decimal places).
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: string (nullable = true)
# MAGIC  |-- stop_timestamp: string (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

############# SOLUTION ###############
from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType

def calculate_total_time(input_df: DataFrame) -> DataFrame:
    seconds_in_one_hour = 3600
    ### YOUR CODE HERE
    stop_timestamp_column_name: str = "stop_timestamp"
    start_timestamp_column_name: str = "start_timestamp"
    ###
    return input_df. \
        withColumn("total_time", col(stop_timestamp_column_name).cast("long")/seconds_in_one_hour - col(start_timestamp_column_name).cast("long")/seconds_in_one_hour). \
        withColumn("total_time", round(col("total_time").cast(DoubleType()),2))
    
display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time)
)



# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_energy
# MAGIC Calculate total_energy (withColumn, cast)
# MAGIC Using [withColumn](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn) and [cast](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Column.cast.html?highlight=cast#pyspark.sql.Column.cast), calculate the total energy by subtracting `meter_stop` from `meter_start`, converting that value from Wh (Watt-hours) to kWh (kilo-Watt-hours), and rounding to the nearest 2 decimal points.
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  ```
# MAGIC
# MAGIC  **Hint:** Wh -> kWh = divide by 1000

# COMMAND ----------

############ SOLUTION ############

def calculate_total_energy(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    meter_stop_column_name: str = "meter_stop"
    meter_start_column_name: str = "meter_start"
    ###
    return input_df \
        .withColumn("total_energy", (col(meter_stop_column_name) - col(meter_start_column_name))/1000) \
        .withColumn("total_energy", round(col("total_energy").cast(DoubleType()),2))
    

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Calculate total_parking_time
# MAGIC In our target object, there is a field `total_parking_time` which is the number of hours that the EV is plugged in but not charging. This denoted in the **Meter Values Request** by the `measurand` = `Power.Active.Import` where `phase` is `None` or `null` and a value of `0`.
# MAGIC
# MAGIC While it might seem easy on the surface, the logic is actually quite complex and will require you to spend some time understanding [Windows](https://sparkbyexamples.com/pyspark/pyspark-window-functions/) in order for you to complete it. Don't worry, take your time to think through the problem!
# MAGIC
# MAGIC We'll need to do this in a handful of steps:
# MAGIC 1. Build a DataFrame from our MeterValue Request data with `transaction_id`, `timestamp`, `measurand`, `phase`, and `value`.
# MAGIC 2. Return only rows with `measurand` = `Power.Active.Import` and `phase` = `Null`
# MAGIC 3. Figure out how to represent in the DataFrame when a Charger is actively charging or not charging, calculate the duration of each of those groups, and sum the duration of the non charging groups as the `total_parking_time`
# MAGIC
# MAGIC **Notes**
# MAGIC * There may be many solutions but the focus should be on using the Spark built-in API
# MAGIC * You should be able to accomplish this entirely in DataFrames without for-expressions
# MAGIC * You'll use the following functions
# MAGIC   * [filter](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.filter.html?highlight=filter#pyspark.sql.DataFrame.filter)
# MAGIC   * [Window](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.Window.html?highlight=window#pyspark.sql.Window)
# MAGIC   * [groupBy](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html?highlight=groupby#pyspark.sql.DataFrame.groupBy)
# MAGIC
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

############## SOLUTION ###############
from pyspark.sql.functions import when, sum, abs, first, last, lag
from pyspark.sql.window import Window

def calculate_total_parking_time(input_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    transaction_id_column_name: str = "transaction_id"
    ###

    window_by_transaction = Window.partitionBy(transaction_id_column_name).orderBy(col("timestamp").asc())
    window_by_transaction_group = Window.partitionBy([transaction_id_column_name, "charging_group"]).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return input_df.\
        withColumn("charging", when(col("value") > 0,1).otherwise(0)).\
        withColumn("boundary", abs(col("charging")-lag(col("charging"), 1, 0).over(window_by_transaction))).\
        withColumn("charging_group", sum("boundary").over(window_by_transaction)).\
        select(col(transaction_id_column_name), "timestamp", "value", "charging", "boundary", "charging_group").\
        withColumn("first", first('timestamp').over(window_by_transaction_group).alias("first_id")).\
        withColumn("last", last('timestamp').over(window_by_transaction_group).alias("last_id")).\
        filter(col("charging") == 0).\
        groupBy(transaction_id_column_name, "charging_group").agg(
            first((col("last").cast("long") - col("first").cast("long"))).alias("group_duration")
        ).\
        groupBy(transaction_id_column_name).agg(
            round((sum(col("group_duration"))/3600).cast(DoubleType()), 2).alias("total_parking_time")
        )

display(meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
    transform(calculate_total_parking_time)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE: Join and Shape
# MAGIC
# MAGIC Join and Shape (left join, select)
# MAGIC
# MAGIC Now that we have the `total_parking_time`, we can join that with our Target Dataframe (where we stored our Stop/Start Transaction data).
# MAGIC
# MAGIC Recall that our newly transformed DataFrame has the following schema:
# MAGIC ```
# MAGIC root
# MAGIC |-- transaction_id: integer (nullable = true)
# MAGIC |-- total_parking_time: double (nullable = true)
# MAGIC ```
# MAGIC  
# MAGIC Target Schema:
# MAGIC ```
# MAGIC root
# MAGIC  |-- charge_point_id: string (nullable = true)
# MAGIC  |-- transaction_id: integer (nullable = true)
# MAGIC  |-- meter_start: integer (nullable = true)
# MAGIC  |-- meter_stop: integer (nullable = true)
# MAGIC  |-- start_timestamp: timestamp (nullable = true)
# MAGIC  |-- stop_timestamp: timestamp (nullable = true)
# MAGIC  |-- total_time: double (nullable = true)
# MAGIC  |-- total_energy: double (nullable = true)
# MAGIC  |-- total_parking_time: double (nullable = true)
# MAGIC ```

# COMMAND ----------

########### SOLUTION ############
def join_and_shape(input_df: DataFrame, joined_df: DataFrame) -> DataFrame:
    ### YOUR CODE HERE
    join_type: str = "left"
    ###
    return input_df.\
        join(joined_df, on=input_df.transaction_id == joined_df.transaction_id, how=join_type).\
        select(
            input_df.charge_point_id, 
            input_df.transaction_id, 
            input_df.meter_start, 
            input_df.meter_stop, 
            input_df.start_timestamp, 
            input_df.stop_timestamp, 
            input_df.total_time, 
            input_df.total_energy, 
            joined_df.total_parking_time
        )

display(stop_transaction_request_df.\
    transform(
        join_with_start_transaction_responses, 
        start_transaction_response_df.\
            transform(match_start_transaction_requests_with_responses, start_transaction_request_df)
    ).\
    transform(calculate_total_time).\
    transform(calculate_total_energy).\
    transform(join_and_shape, meter_values_request_df.filter((col("measurand") == "Power.Active.Import") & (col("phase").isNull())).\
        transform(calculate_total_parking_time)
     )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reflect
# MAGIC Congratulations on finishing the Gold Tier exercise! Compared to a previous exercise where we did some of these exact exercises, you might have noticed that this time around, it was significantly easier to comprehend and complete because we didn't need to perform as many repetitive transformations to get to the interesting business logic.
# MAGIC
# MAGIC * What might you do with this data now that you've transformed it?

# COMMAND ----------


