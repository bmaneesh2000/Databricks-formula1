-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula1
URL 'abfss://formula1@sample20001.dfs.core.windows.net'
WITH (STORAGE CREDENTIAL `actest`)
COMMENT 'External location for the formula1 container';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@sample20001.dfs.core.windows.net/landing'

-- COMMAND ----------

Show catalogs

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1
   MANAGED LOCATION 'abfss://formula1@sample20001.dfs.core.windows.net/' 
   COMMENT 'This is the main catalog for the formula1 project' ;

-- COMMAND ----------

show catalogs

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
CREATE SCHEMA IF NOT EXISTS formula1.bronze
    MANAGED LOCATION 'abfss://formula1@sample20001.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1.silver
    MANAGED LOCATION 'abfss://formula1@sample20001.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1.gold
    MANAGED LOCATION 'abfss://formula1@sample20001.dfs.core.windows.net/gold';      

-- COMMAND ----------

use catalog formula1;

-- COMMAND ----------

show schemas

-- COMMAND ----------

CREATE EXTERNAL VOLUME formula1.landing.files
LOCATION 'abfss://formula1@sample20001.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files