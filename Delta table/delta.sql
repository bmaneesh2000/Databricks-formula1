-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating and accessing delta tables

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo
MANAGED LOCATION 'abfss://demo@sample20001.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo.delta_lake
MANAGED LOCATION 'abfss://demo@sample20001.dfs.core.windows.net/delta_lake'


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.companies
(
  company_name STRING,
  founded_date DATE,
  country      STRING
)
USING DELTA;


-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies;


-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
VALUES ('Apple', '1976-04-01', 'USA')

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies;


-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
VALUES
  ('Microsoft', '1975-04-04', 'USA'),
  ('Google',  '1998-09-04', 'USA'),
  ('Amazon',  '1994-07-05', 'USA');

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## timetravel and history

-- COMMAND ----------

DESCRIBE HISTORY demo.delta_lake.companies;


-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies;

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies
VERSION AS OF 1;

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies
TIMESTAMP AS OF '2026-09-19T15:45:38.000+00:00';

-- COMMAND ----------

RESTORE TABLE demo.delta_lake.companies VERSION AS OF 1;


-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies;


-- COMMAND ----------

DESC HISTORY demo.delta_lake.companies;