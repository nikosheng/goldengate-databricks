# Databricks notebook source
import dlt
from pyspark.sql.functions import *
##Create the bronze information table containing the parquet data taken from the storage
@dlt.create_table(name="ocigg_cdc",
                  comment = "New data incrementally ingested from GoldenGate")
def ocigg_cdc():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/mnt/ogg/OGG.OGGTEST.T5"))

# COMMAND ----------

#This could also be a view: create_view
@dlt.create_table(name="ocigg_cdc_clean",
                  comment="Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type")
@dlt.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
@dlt.expect_or_drop("valid_id", "ID IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "optype IN ('I', 'D', 'U')")
def customers_cdc_clean():
  return dlt.read_stream("ocigg_cdc") \
            .select("ADDRESS", "NAME", "ID", "optype", "timestamp", "_rescued_data")

# COMMAND ----------

dlt.create_streaming_table(name="ocigg_final", comment="Clean, materialized OCI GoldenGate CDC data")

# COMMAND ----------

dlt.apply_changes(
  target = "ocigg_final", #The table being materilized
  source = "ocigg_cdc_clean", #the incoming CDC
  keys = ["ID"], #what we'll be using to match the rows to upsert
  sequence_by = col("timestamp"), #we deduplicate by operation date getting the most recent value
  ignore_null_updates = False,
  apply_as_deletes = expr("optype = 'D'"), #DELETE condition
  except_column_list = ["optype", "_rescued_data"]) #in addition we drop metadata columns

# COMMAND ----------


