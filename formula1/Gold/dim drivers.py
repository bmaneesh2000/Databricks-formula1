# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_df               = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_drivers_df = (
    drivers_df
        .join(
            ref_nationality_region_df,
            drivers_df.nationality == ref_nationality_region_df.nationality,
            "left"
        )
        .select(
            drivers_df.driver_id,
            drivers_df.driver_name,
            drivers_df.date_of_birth,
            drivers_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

display(dim_drivers_df)

# COMMAND ----------

(
    dim_drivers_df
        .write
        .format("delta")
        .mode("overwrite")             
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))