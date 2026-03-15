# Databricks notebook source
# MAGIC %md 
# MAGIC ##### PROCESS ADDRESSES DATA --in Pyhton
# MAGIC
# MAGIC 1. Ingest the data into data lakehouse bronze_addresses
# MAGIC 2. Perform data quality check and transform the data as required sliver_addresses_clean
# MAGIC 3. Apply changes to the address data sliver_addresses

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### 1. Ingest data into bronze_address table
# MAGIC
# MAGIC To read data from files and create a LIVE table in python we use 
# MAGIC @dlt.table() --decorator to create a LIVE table
# MAGIC this tells databrciks this is a managed DLT tabel which will take data from  the returned dataframe of below function
# MAGIC
# MAGIC ###### SYNTAX
# MAGIC @dlt.table( <br>
# MAGIC     name='table_name' [, catalog='',schema=''] <br>
# MAGIC     comment ='' <br>
# MAGIC     table_properties={'key' : 'value'} <br>
# MAGIC ) <br>
# MAGIC def \<function_name> (): <br>
# MAGIC     return ( dataframe)
# MAGIC
# MAGIC Note: if we haven't specify the table name function will bacame LIVE table name 

# COMMAND ----------

# MAGIC %md 
# MAGIC Add new new column to bronze_addresses table <br>
# MAGIC input_file_path <br>
# MAGIC ingestion_date

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
    name='bronze_addresses',
    comment ='raw data form files to lakehouse',
    table_properties= {'quality':'bronze'}
)
def pull_bronze_addresses_data():
    return (
        spark.readStream 
            .format('cloudFiles')
            .option('cloudFiles.format','csv')
            .option('cloudFiles.inferColumnTypes','true')
            .load('/Volumes/circuitbox/landing/operational_data/addresses/')
            .select(  "*", #'customer_id','address_line_1','city','state','postcode','created_date',
                    col('_metadata.file_path').alias('input_file_path'),
                    current_timestamp().alias('ingestion_date')
                    )
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### 2. Perform data quality checks aand transform data  from bronze table to sliver_addresses_clean table 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### For data Quality check we use <br>
# MAGIC For one condition <br>
# MAGIC @dlt.expect <br>
# MAGIC @dlt.expect_or_drop <br>
# MAGIC @dlt.expect_or_fail <br>
# MAGIC For multiple condtions <br>
# MAGIC @dlt.expect_all <br>
# MAGIC @dlt.expect_all_or_drop <br>
# MAGIC @dlt.expect_all_or_fail <br>
# MAGIC
# MAGIC for all these we need to pass string with name as key and condition as value <br>
# MAGIC ( name str , condtion str) <br>
# MAGIC  and we can add these in between the table decorator and function 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Data quality check for sliver_customer_clean live table 
# MAGIC
# MAGIC 1. Fail if customer_id is NULl
# MAGIC 2. Drop records with address_line as NULL
# MAGIC 3. Warn if postcode is less than 5 characters
# MAGIC
# MAGIC ###### Transformations
# MAGIC 1. CAST created_date to DATE

# COMMAND ----------

@dlt.table(
    name='sliver_addresses_clean',
    comment = 'cleaned data from bronze table',
    table_properties = {'quality' : 'sliver'}
)
@dlt.expect_or_fail('valied_customer','customer_id IS NOT NULL')
@dlt.expect_or_drop('valid_address','address_line_1 IS NOT NULL')
@dlt.expect('valied_postcode','length(postcode)=5')
def pull_bronze_data():
    return(
        dlt.read_stream('bronze_addresses')  # we can use spark.reradStream.table() recommended with LIVE keyword
           .select('customer_id','address_line_1','city','state','postcode',
                    col('created_date').cast('date') )
    ) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### 3. Apply changes to the sliver_addresses table 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Using @dlt.apply_changes() decorator 
# MAGIC
# MAGIC By using this dlt decorator we will load the CDC data to target table as SCD type data 
# MAGIC
# MAGIC ###### SYNTAX 
# MAGIC @dlt.apply_changes( <br>
# MAGIC   target='table_name', <br>
# MAGIC   source= 'source-table_name', <br>
# MAGIC   keys= ['col_name'] ,<br>
# MAGIC   sequece_by ='sequenced col name', <br>
# MAGIC   stored_as_scd_type= 1 or 2  <br>
# MAGIC )
# MAGIC
# MAGIC Note: target table must be created upfront 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### To create dlt streaming table we use 
# MAGIC ###### dlt.create_streaming_table()
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC No need to use LIVE.\<table_name> if we are using dlt functions <br>
# MAGIC LIVE does not exist in python <br>
# MAGIC Table names are implicitly LIVE <br>
# MAGIC You reference tables by plain string names
# MAGIC
# MAGIC ####### BUT 
# MAGIC If we are using spark.read_stream.table() then LIVE is required otherwise DLT doesn't know the tbl is from current pipeline or not DAG may not be created properly 

# COMMAND ----------

dlt.create_streaming_table(
    name='sliver_addresses',
    comment='cleaned and transfromed data for sliver layer',
    table_properties={'quality':'sliver'}
)
dlt.apply_changes(
    target='sliver_addresses',
    source='sliver_addresses_clean',
    keys=['customer_id'],
    sequence_by ='created_date',
    stored_as_scd_type = 2
)