# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

catalog_name

# COMMAND ----------

sf= f"{landing_folder_path}/races.csv"
table = f"{catalog_name}.{bronze_schema}.races"

table

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



races_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
#         .option('inferSchema', 'true')
         .option('mode', 'FAILFAST')
         .schema(races_schema)
         .load(sf)
)



display(races_df)

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)


# COMMAND ----------

(
    races_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table)
)



# COMMAND ----------

display(spark.table(table))