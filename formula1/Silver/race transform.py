# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

bt = f"{catalog_name}.{bronze_schema}.races"
st = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql import functions as F
races_df = spark.table(bt)


# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("Ingetion_timestamp"),
    F.col("Sourcefile")
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "raceName": "race_name",
            "date": "race_date",
            "Ingetion_timestamp" :"ingestion_timestamp",
            "Sourcefile":"source_file"
        })
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

display(races_distinct_df)

# COMMAND ----------

races_final_df = (
    races_distinct_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

display(races_final_df)

# COMMAND ----------

(
    races_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(st)
)


display(spark.table(st))