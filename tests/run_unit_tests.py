# Databricks notebook source
pip install git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

working_directory = '/FileStore/vijdurga.n/batch_processing_bronze_ingest'

# COMMAND ----------

from exercise_ev_databricks_unit_tests.batch_processing_bronze import test_write_e2e

test_write_e2e(dbutils.fs.ls(f"{working_directory}/output"), spark, display)

# COMMAND ----------


