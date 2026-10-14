# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

sr = f"{landing_folder_path}/constructors.json"
table = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

constructors_schema = """constructorId STRING, 
                         name STRING, 
                         nationality STRING, 
                         url STRING
                         """

# COMMAND ----------

constructors_df = (
    spark.read
       .format('json')
       .schema(constructors_schema)
       .option('mode', 'FAILFAST')
       .load(sr)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

ndf = add_ingestion_metadata(constructors_df)

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