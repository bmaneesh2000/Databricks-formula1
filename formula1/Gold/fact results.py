# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
         .withColumn("session_type", F.lit("RACE"))
         .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
)

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
         .withColumn("session_type", F.lit("SPRINT"))
         .drop("race_name", "race_date", "ingestion_timestamp", "source_file")
)

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

fact_session_results_df = (
    results_sprints_df
        .withColumn("is_win", F.col("final_position") == 1)
        .withColumn("is_podium", F.col("final_position").between(1, 3))
        .withColumn("has_points", F.col("points") > 0)
)


display(fact_session_results_df.filter("season = 2025"))

# COMMAND ----------

(
    fact_session_results_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)



display(spark.table(target_table))