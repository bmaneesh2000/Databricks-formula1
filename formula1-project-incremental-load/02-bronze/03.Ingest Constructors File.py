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
source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# Define the schema
constructors_schema = """constructorId STRING, 
                         name STRING, 
                         nationality STRING, 
                         url STRING
                         """

# COMMAND ----------

# Read data from the constructors file
constructors_df = (
    spark.read
       .format('json')
       .schema(constructors_schema)
       .option('mode', 'FAILFAST')
       .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

write_to_bronze (
    input_df = constructors_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))