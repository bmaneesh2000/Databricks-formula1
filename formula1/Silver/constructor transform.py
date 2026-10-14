# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F

constructors_df = spark.table(bronze_table)


# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")


# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "name": "constructor_name"
        })
)

display(constructors_renamed_df)

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

display(constructors_distinct_df)

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "name": "constructor_name"
        })
)

display(constructors_renamed_df)

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

display(constructors_distinct_df)

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)

display(constructors_final_df)


# COMMAND ----------

(
    constructors_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
)

display(spark.table(silver_table))