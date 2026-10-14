# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")


# COMMAND ----------

dim_constructors_df = (
    constructors_df
        .join(
            ref_nationality_region_df,
            constructors_df.nationality == ref_nationality_region_df.nationality,
            "left"
            )
        .select (
            constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

(
    dim_constructors_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)

display(spark.table(target_table))