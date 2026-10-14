# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

sr = f"{landing_folder_path}/drivers.json"
table = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

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

ndf = (
    spark.read
       .format('json')
       .schema(drivers_schema)
       .option('mode', 'FAILFAST')
       .load(sr)
)

# COMMAND ----------

ndf = add_ingestion_metadata(ndf)


# COMMAND ----------

(
    ndf
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table)
)

# COMMAND ----------

display(spark.table(table))