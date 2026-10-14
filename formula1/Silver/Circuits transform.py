# Databricks notebook source
# MAGIC %run ../common/env_config

# COMMAND ----------

# MAGIC %run ../common/helper

# COMMAND ----------

bt = f"{catalog_name}.{bronze_schema}.circuits"
st = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

cdf=spark.read.table(bt)

# COMMAND ----------

cdf=spark.table(bt)

# COMMAND ----------

cdf=spark.read.option("versionAsOf",0).table(bt)

# COMMAND ----------

cdf=spark.read.table(bt)

# COMMAND ----------

display(cdf)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df = cdf.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("Ingetion_timestamp"),
    F.col("Sourcefile")
)

# COMMAND ----------

# DBTITLE 1,Cell 11
circuits_selected_df = cdf.selectExpr(
    "circuitId",
    "circuitName",
    "lat",
    "long",
    "locality",
    "country",
    "Ingetion_timestamp AS ingestion_timestamp",
    "Sourcefile AS source_file"
)

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "circuitName": "circuit_name",
            "lat": "latitude",
            "long": "longitude"
        })
)



display(circuits_renamed_df)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

display(circuits_distinct_df)

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
        .withColumn('circuit_name', F.initcap(F.col("circuit_name")))
        .withColumn('locality', F.initcap(F.col("locality")))
)

# COMMAND ----------

(
    circuits_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(st)
)


display(spark.table(st))