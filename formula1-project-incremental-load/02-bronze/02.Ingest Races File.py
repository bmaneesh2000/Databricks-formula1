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

source_file = f"{landing_folder_path}/{v_batch_id}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

races_schema = StructType([
    StructField('season',   IntegerType()),
    StructField("round",    IntegerType()),
    StructField("url",      StringType()),
    StructField("raceName", StringType()),
    StructField("date",     DateType()),
    StructField("circuitId", StringType())
])

# COMMAND ----------

races_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
#         .option('inferSchema', 'true')
         .option('mode', 'FAILFAST')
         .schema(races_schema)
         .load(source_file)
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE formula1_incr.bronze.circuits;

# COMMAND ----------



# COMMAND ----------

write_to_bronze (
    input_df = races_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))