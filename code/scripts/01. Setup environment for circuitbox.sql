-- Databricks notebook source

CREATE EXTERNAL LOCATION IF NOT EXISTS dbk_course_ext_dl_circuitbox
URL 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net'
WITH (STORAGE CREDENTIAL dbk_course_ext_sc)
COMMENT 'external location for circuitbox data lakehouse'


CREATE CATALOG IF NOT EXISTS circuitbox
MANAGED LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/'
COMMENT 'catalog for the circuitbox data lakehouse'

show catalogs


USE CATALOG circuitbox;
CREATE SCHEMA IF NOT EXISTS landing 
MANAGED LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/landing';

CREATE SCHEMA IF NOT EXISTS lakehouse
MANAGED LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/lakehouse'


use catalog circuitbox;
show schemas

USE CATALOG circuitbox;
USE SCHEMA landing;


CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
LOCATION 'abfss://circuitbox@dbkstorageextdl.dfs.core.windows.net/landing/operational_data'
COMMENT 'volume for operational data lakehouse'

