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

from pyspark.sql import functions as F

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table)

# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter((F.col("batch_id")== v_batch_id ))
    )

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# circuits_selected_df = circuits_df.select(
#     "circuitId",
#     "circuitName",
#     "lat",
#     "long",
#     "locality",
#     "country",
#     "ingestion_timestamp",
#     "source_file"
# )

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# circuits_renamed_df = (
#     circuits_selected_df
#         .withColumnRenamed("circuitId", "circuit_id")
#         .withColumnRenamed("circuitName", "circuit_name")
#         .withColumnRenamed("lat", "latitude")
#         .withColumnRenamed("long", "longitude")
# )

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

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter(
#     "circuit_id IS NOT NULL"
# )

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
        .withColumn('circuit_name', F.initcap(F.col("circuit_name")))
        .withColumn('locality', F.initcap(F.col("locality")))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# circuits_final_df = (
#     circuits_final_df
#         .withColumn("created_timestamp", F.current_timestamp())
#         .withColumn("updated_timestamp", F.current_timestamp())
# )

# COMMAND ----------

# if not spark.catalog.tableExists(silver_table):
#     (
#         circuits_final_df
#             .write
#             .format("delta")
#             .mode("overwrite")
#             .saveAsTable(silver_table)
#     )

# else:
#     from delta.tables import DeltaTable

#     delta_table = DeltaTable.forName(spark, silver_table)
#     (
#         delta_table.alias("t")
#         .merge (
#             circuits_final_df.alias("s"),
#             "t.circuit_id = s.circuit_id"
#         )
#         .whenMatchedUpdate(
#             condition="s.batch_id >= t.batch_id",
#             set = {
#                     "circuit_name": "s.circuit_name",
#                     "latitude": "s.latitude",
#                     "longitude": "s.longitude",
#                     "locality": "s.locality",
#                     "country": "s.country",
#                     "ingestion_timestamp": "s.ingestion_timestamp",
#                     "source_file": "s.source_file",
#                     "batch_id": "s.batch_id",
#                     "updated_timestamp": "s.updated_timestamp"              
#             }
#         )
#         .whenNotMatchedInsertAll()
#         .execute()
#     )

# COMMAND ----------

write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_id = s.circuit_id",
    columns_to_update=[
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id"        
    ]
)

# COMMAND ----------

display(spark.table(silver_table))