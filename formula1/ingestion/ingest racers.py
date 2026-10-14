# Databricks notebook source
rdf= spark.read.format("csv").option("header","true").load("/Volumes/formula1/landing/files/races.csv")

# COMMAND ----------

display(rdf)

# COMMAND ----------

rdf

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

race_schema = StructType([
    StructField('season',   IntegerType()),
    StructField("round",    IntegerType()),
    StructField("url", StringType()),
    StructField("raceName", StringType()),
    StructField("date", DateType()),
    StructField("circuitId",StringType()),
])

# COMMAND ----------

# DBTITLE 1,Cell 5
rdf= spark.read.format("csv").option("header","true").schema(race_schema).load("/Volumes/formula1/landing/files/races.csv")

# COMMAND ----------

from pyspark.sql import functions as F

nrdf=rdf.withColumn("Ingetion_timestamp",F.current_timestamp()).withColumn("Sourcefile",F.col("_metadata.file_path"))

display(nrdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS formula1.bronze.races;

# COMMAND ----------

(
    nrdf
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('formula1.bronze.races')
)

# COMMAND ----------

display(spark.table('formula1.bronze.races'))