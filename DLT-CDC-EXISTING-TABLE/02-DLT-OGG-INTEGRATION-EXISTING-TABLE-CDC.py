# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the bronze information table containing the raw CDC data taken from the storage

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

##Create the bronze information table containing the raw CDC data taken from the storage
@dlt.create_table(name="ocigg_cdc",
                  comment = "New data incrementally ingested from GoldenGate")
def ocigg_cdc():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/mnt/ogg/ogg/OGG.OGGTEST.T/cdc/delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleansed cdc data, tracking data quality with a view.

# COMMAND ----------

@dlt.create_table(name="ocigg_cdc_clean",
                  comment="Cleansed cdc data, tracking data quality with a view.")
@dlt.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
@dlt.expect_or_drop("valid_id", "ID IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "optype IN ('I', 'D', 'U', 'T')")
def customers_cdc_clean():
  return dlt.read_stream("ocigg_cdc") \
            .select("ADDRESS", "NAME", "ID", "optype", "timestamp", "_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before recreating the target streaming table, delete the checkpoint directory of the existing streaming table, which is stored in the object storage you defined in the Delte Live Table pipeline setting. For more information, please refer to [specify-a-storage-location](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/settings#specify-a-storage-location)

# COMMAND ----------


dlt.create_streaming_table(name="ocigg_existing_streaming_table", comment="Clean, materialized OCI GoldenGate CDC target streaming table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply the CDC changes to the existing target streaming table

# COMMAND ----------

dlt.apply_changes(
  target = "ocigg_existing_streaming_table", #The target streaming table
  source = "ocigg_cdc_clean", #the incoming CDC
  keys = ["ID"], #what we'll be using to match the rows to upsert
  sequence_by = col("timestamp"), #we deduplicate by operation date getting the most recent value
  ignore_null_updates = False,
  apply_as_deletes = expr("optype = 'D'"), #DELETE condition
  apply_as_truncates = expr("optype = 'T'"), #TRUNCATE condition
  except_column_list = ["optype", "_rescued_data"]) #in addition we drop metadata columns
