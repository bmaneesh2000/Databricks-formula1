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

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_df = (
    spark.table(bronze_table)
         .filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "raceName": "race_name",
            "date": "race_date"
        })
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

races_final_df = (
    races_distinct_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))