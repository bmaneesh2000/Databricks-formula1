# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

catalog_name

# COMMAND ----------

sf= f"{landing_folder_path}/circuits.csv"
table = f"{catalog_name}.{bronze_schema}.circuits"

table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId',   StringType()),
    StructField("url",         StringType()),
    StructField("circuitName", StringType()),
    StructField("lat",         DoubleType()),
    StructField("long",        DoubleType()),
    StructField("locality",    StringType()),
    StructField("country",     StringType())
])

circuits_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .schema(circuits_schema)
         .load(sf)
)

circuits_df

# COMMAND ----------

ndf = add_ingestion_metadata(circuits_df)




# COMMAND ----------

display(ndf)

# COMMAND ----------

# DBTITLE 1,Cell 8
(
    ndf
        .withColumnRenamed('ingestion_timestamp', 'Ingetion_timestamp')
        .withColumnRenamed('source_file', 'Sourcefile')
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table)
)