-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ##### PROCESS ORDERS DATA
-- MAGIC
-- MAGIC 1. Ingest the data into the data lakehouse -- bronze_order
-- MAGIC
-- MAGIC 2. Perform data quality checks and transform the data as required -- Sliver_order_clean
-- MAGIC
-- MAGIC 3. Explode the items array from the orders object -- Sliver_orders

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### 1. read data to bronze table from ADLS as stream table 
-- MAGIC
-- MAGIC Add columns for 
-- MAGIC 1. input_file_path
-- MAGIC 2. ingestion_timestamp

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw data from files to data lakehouse'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT *,
_metadata.file_path as input_file_path,
CURRENT_TIMESTAMP as ingestion_timestamp
FROM cloud_files(
  '/Volumes/circuitbox/landing/operational_data/orders/',
  'json',
  map('cloudFiles.inferColumnTypes','true',
      'cloudFiles.schemaLocation','/Volumes/circuitbox/landing/operational_data/schema/orders/',
      'cloudFiles.schemaHints',
      'items ARRAY<STRUCT<
    category STRING,
    details STRUCT<brand STRING, color STRING>,
    item_id BIGINT,
    name STRING,
    price BIGINT,
    quantity BIGINT
>>')
);



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### 2. sliver_ordeer_clean table 
-- MAGIC By using LIVE bronze table which is created above we need to create a sliver table with data quality expectations and trnasformations like CASTING column to proper data type 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### Data Quality check ( Expectations)
-- MAGIC 1. fail if customer_id is NULL
-- MAGIC 2. Fail if order_id is NULL
-- MAGIC 3. Warn if Order_status is not in pending, shipped, cancelled, completed
-- MAGIC 4. Warn if payment_method is not one of credit card, paypal, bank tranfer 
-- MAGIC
-- MAGIC ###### Transformations
-- MAGIC 1. CAST order_timestamp to TIMESTAMP

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_orders_clean(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_status EXPECT (order_status  IN ('pending','shipping','cancelled','completed')),
  CONSTRAINT valid_payment_methos EXPECT (payment_method IN ('Bank transfer','Paypal','credit card'))
)
COMMENT 'cleaned data from bronze table'
TBLPROPERTIES ('quality'='sliver')
AS
SELECT order_id,customer_id,CAST(order_timestamp as timestamp) as order_timestamp,
order_status,payment_method,items
FROM STREAM(LIVE.bronze_orders);



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### 3. Explode the array items in order data
-- MAGIC By using explode() we can explode the array items to seperate rows for each record

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE sliver_orders
(
  CONSTRAINT valid_item_id EXPECT (item_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'cleaned data from sliver_order_clean table'
TBLPROPERTIES ('quality'='sliver')
AS
SELECT order_id,
       customer_id,
       order_timestamp,
       order_status,
       payment_method,
       item.item_id,
       item.name as item_name,
       item.price as item_price,
       item.quantity as item_quantity,
       item.category as item_category
    FROM 
    (
      SELECT order_id,customer_id,order_timestamp,order_status,payment_method,
      explode(items) as item
      FROM STREAM(LIVE.sliver_orders_clean)
    );

