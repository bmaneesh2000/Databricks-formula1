# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

from pyspark.sql import functions as F

# COMMAND ----------

sprints_df = (
  spark.table(bronze_table)
       .select("season",
              "round",
              "constructorId",
              "driverId",
              "date",
              "raceName",
              "grid",
              "laps",
              "number",
              "points",
              "position",
              "positionText",
              "status",
              "Ingetion_timestamp",
              "Sourcefile")
       .withColumnsRenamed({
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            "date": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "final_position",
            "positionText": "final_position_text",
            "Ingetion_timestamp": "ingestion_timestamp",
            "Sourcefile": "source_file"
        })
)

# COMMAND ----------

sprints_valid_df = (
    sprints_df
        .filter(
            F.col("season").isNotNull() &
            F.col("round").isNotNull() &
            F.col("constructor_id").isNotNull() &
            F.col("driver_id").isNotNull() 
        )
        .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
)


display(sprints_df.count() - sprints_valid_df.count())

# COMMAND ----------

sprints_final_df = (
    sprints_valid_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
)

display(sprints_final_df)


# COMMAND ----------

(
    sprints_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

display(spark.table(silver_table))