# Databricks notebook source
# MAGIC %md
# MAGIC ### Reload the existing table

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# Reload the existing table
@dlt.table(name = "ocigg_existing_table")
def existing_table_reload():
  return spark.table("oci.t4_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the target streaming table

# COMMAND ----------

# Create the target streaming table
dlt.create_streaming_table(name="ocigg_existing_streaming_table", comment="Create the target streaming table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Re-apply the existing table data to the target streaming table

# COMMAND ----------

# Re-apply the existing table data to the target streaming table
dlt.apply_changes(
  target = "ocigg_existing_streaming_table", #Target streaming table
  source = "ocigg_existing_table", #The existing table
  keys = ["ID"],
  sequence_by = col("timestamp"),
  ignore_null_updates = False)
