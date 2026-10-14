# Databricks notebook source

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df.withColumn('Ingetion_timestamp', F.current_timestamp())
          .withColumn('Sourcefile', F.col('_metadata.file_path'))
    )