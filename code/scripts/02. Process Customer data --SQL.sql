-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #### Process Customer data 
-- MAGIC 1. Ingest the data into data lakehouse bronze_customers
-- MAGIC 2. Perform data quality check and transform the data as required sliver_customer_clean 
-- MAGIC 3. Apply changes to the customer data sliver_customers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### 1. Ingest data into data lakehouse bronze_customer  as a streaming table 

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'Raw customers data ingested from the source system operational_data'
TBLPROPERTIES ('quality'='bronze')
AS SELECT *,
        _metadata.file_path AS input_file_path,
        CURRENT_TIMESTAMP as ingestion_timestamp
FROM cloud_files(
            '/Volumes/circuitbox/landing/operational_data/customers/',
            'json',
            map('cloudFiles.inferColumnTypes','true')
);

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### 2. Perform data quality checks and transformations and load data to sliver_customer_clean table 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For SQL we have 
-- MAGIC
-- MAGIC CONSTRAINT \<constraint_name> EXPECT \<condition for columns> ON VIOLATION [ FAIL UPDATE | DROP ROW ] --empty for WARN

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### Data quality check for sliver_customer_clean live table 
-- MAGIC
-- MAGIC 1. Fail if customer_id is NULl
-- MAGIC 2. Drop records with customer_name as NULL
-- MAGIC 3. Warn if telephone is less than 10 characters
-- MAGIC 4. Warn if email is NULL
-- MAGIC 5. Warn if date_of_birth is before 1920
-- MAGIC
-- MAGIC ###### Transformations
-- MAGIC 1. CAST date_of_birth to DATE
-- MAGIC 2. CAST created_date to DATE

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_customer_clean
(
  CONSTRAINT valid_cust_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_cust_name EXPECT (customer_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email EXPECT (email is NOT NULL),
  CONSTRAINT valid_telephone EXPECT (LENGTH(telephone)>=10),
  CONSTRAINT valid_date_of_birth EXPECT (date_of_birth > '1920-01-01')  
)
COMMENT 'cleaned data for sliver layer'
TBLPROPERTIES ('quality'='sliver')
AS
SELECT customer_id,customer_name,CAST(date_of_birth as DATE) as date_of_birth
,email,telephone,
CAST(created_date AS DATE) as created_date
FROM STREAM(LIVE.bronze_customers); --- here we are using STREAM() to loead the data incrementally to sliver_table
--- incremental data: loading only the new records every time
--- LIVE keyword ro call the table which is created in same DLT pipeline 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### 3. Apply changes to sliver_customer table --SCD type 1 (no history only latest data)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### APPLY CHANGES API 
-- MAGIC
-- MAGIC It is used to indicate the databricks that we are providing CDC data (changed data capture) please maintain an SCD table correclty. Used for Streaming or LIVE table 
-- MAGIC
-- MAGIC ####### SYNTAX
-- MAGIC APPLY CHANGES INTO \<target_table_name> <br>
-- MAGIC FROM \<source_table\> <br>
-- MAGIC KEYS (primary_key) <br>
-- MAGIC SEQUENCED BY \<sequenced_column> <br>
-- MAGIC STORED AS SCD TYPE 1|2 --1 by default
-- MAGIC
-- MAGIC SCD-1 : for distinct one KEY keep only one record based on created_date 
-- MAGIC
-- MAGIC SCD-2 : for distinct one KEY keep history records as well and make one as active based on created_date (__START_AT , __END_AT)
-- MAGIC
-- MAGIC Note: target table must be created upfront

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_customers;
 --- we can specify data quality checks as well

-- COMMAND ----------

APPLY CHANGES INTO sliver_customers
FROM STREAM(LIVE.sliver_customer_clean)
KEYS(customer_id)
SEQUENCE BY created_date
STORED AS SCD TYPE 1;