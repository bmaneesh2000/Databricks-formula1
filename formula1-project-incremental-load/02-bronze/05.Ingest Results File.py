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
source_file = f"{landing_folder_path}/{v_batch_id}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

results_schema = StructType([
    StructField("date", DateType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])

# COMMAND ----------

# Read data from the results file
results_df = (
    spark.read
       .format('json')
       .schema(results_schema)
       .option('mode', 'FAILFAST')
       .load(source_file)
)

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

write_to_bronze (
    input_df = results_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))