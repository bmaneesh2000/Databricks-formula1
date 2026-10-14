# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql import functions as F


drivers_df = spark.table(bronze_table)


# COMMAND ----------

drivers_dropped_df = drivers_df.drop(F.col("url"))

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
        .withColumnsRenamed({
            "driverId": "driver_id",
            "dateOfBirth": "date_of_birth"
        })
)

display(drivers_renamed_df)

# COMMAND ----------

drivers_concatenated_df = (
  drivers_renamed_df
       .withColumn("driver_name", 
                   F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
       .drop("name")
)

display(drivers_concatenated_df)

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])

display(drivers_distinct_df)

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)

display(drivers_final_df)

# COMMAND ----------

(
    drivers_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

display(spark.table(silver_table))