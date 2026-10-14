# Databricks notebook source
# MAGIC %md
# MAGIC #Circuits ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ingestion

# COMMAND ----------


circuits_df= spark.read.format("csv").load("/Volumes/formula1/landing/files/circuits.csv")

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

cdf= spark.read.format("csv").option("header","true").load("/Volumes/formula1/landing/files/circuits.csv")

display(cdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Specify schema
# MAGIC

# COMMAND ----------

cdf= spark.read.format("csv").option("header","true").load("/Volumes/formula1/landing/files/circuits.csv")

cdf

# COMMAND ----------

cdf= spark.read.format("csv").option("header","true").option("inferSchema","True").load("/Volumes/formula1/landing/files/circuits.csv")

cdf

# COMMAND ----------

#manually define schema

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId',   StringType()),
    StructField("url",         StringType()),
    StructField("circuitName", StringType()),
    StructField("lat",         DoubleType()),
    StructField("long",        DoubleType()),
    StructField("locality",    StringType()),
    StructField("country",     StringType())
])

circuits_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .schema(circuits_schema)
         .load("/Volumes/formula1/landing/files/circuits.csv")
)

# COMMAND ----------

#Pipeline fails if bad data is encountered
circuits_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'FAILFAST') 
         .schema(circuits_schema)
         .load("/Volumes/formula1/landing/files/circuits.csv")
)

#pipeline contineues even if bad data is encounterd
circuits_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'PERMISSIVE') 
         .schema(circuits_schema)
         .load("/Volumes/formula1/landing/files/circuits.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## add metadata info to data

# COMMAND ----------

from pyspark.sql import functions as F

ndf = circuits_df.withColumn("Ingetion timestamp",F.current_timestamp())

display(ndf)

# COMMAND ----------

ndf = circuits_df.withColumn("Ingetion_timestamp",F.current_timestamp()).withColumn("Sourcefile",F.col("_metadata.file_path"))

display(ndf)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to delta table

# COMMAND ----------

(
    ndf
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable('formula1.bronze.circuits')
)

# COMMAND ----------

display(spark.table('formula1.bronze.circuits'))