-- Databricks notebook source


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

