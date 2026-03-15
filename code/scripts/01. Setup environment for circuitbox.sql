-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #### Set up Environment for circuitbox project 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### 1. Create external location to circuitbox container 
-- MAGIC
-- MAGIC external location name : dbk_course_ext_dl_circuitbox
-- MAGIC
-- MAGIC ADLS path  : abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net
-- MAGIC
-- MAGIC Storage credential : dbk_course_ext_sc
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS dbk_course_ext_dl_circuitbox
URL 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net'
WITH (STORAGE CREDENTIAL dbk_course_ext_sc)
COMMENT 'external location for circuitbox data lakehouse'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### 2. create catalog 
-- MAGIC
-- MAGIC catalog name : circuitbox
-- MAGIC
-- MAGIC managed location : abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS circuitbox
MANAGED LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/'
COMMENT 'catalog for the circuitbox data lakehouse'

-- COMMAND ----------

show catalogs

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### 3. create schema
-- MAGIC
-- MAGIC schema names : landing and lakehouse
-- MAGIC
-- MAGIC managed locations : abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/landing/ and 
-- MAGIC abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/lakehouse/

-- COMMAND ----------

USE CATALOG circuitbox;
CREATE SCHEMA IF NOT EXISTS landing 
MANAGED LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/landing';

CREATE SCHEMA IF NOT EXISTS lakehouse
MANAGED LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/lakehouse'

-- COMMAND ----------

use catalog circuitbox;
show schemas

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### 4. create volumn
-- MAGIC
-- MAGIC volumn name : operational_data
-- MAGIC
-- MAGIC ADLS path : abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/landing/operational_data

-- COMMAND ----------

USE CATALOG circuitbox;
USE SCHEMA landing;

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/landing/operational_data'
COMMENT 'volume for operational data lakehouse'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls '/Volumes/circuitbox/landing/operational_data/'

-- COMMAND ----------

