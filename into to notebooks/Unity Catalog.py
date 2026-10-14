# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC Select current_metastore()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_demo
# MAGIC URL 'abfss://demo@sample20001.dfs.core.windows.net'
# MAGIC WITH (STORAGE CREDENTIAL `actest`)
# MAGIC COMMENT 'External location for the demo container';

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls 'abfss://demo@sample20001.dfs.core.windows.net'