# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# Define source_file and table_name
source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

# Read data from the drivers file
drivers_df = (
    spark.read
       .format('json')
       .schema(drivers_schema)
       .option('mode', 'FAILFAST')
       .load(source_file)
)

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

write_to_bronze (
    input_df = drivers_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))