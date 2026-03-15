-- Databricks notebook source


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



CREATE OR REFRESH STREAMING TABLE sliver_customers;
 --- we can specify data quality checks as well


APPLY CHANGES INTO sliver_customers
FROM STREAM(LIVE.sliver_customer_clean)
KEYS(customer_id)
SEQUENCE BY created_date
STORED AS SCD TYPE 1;
