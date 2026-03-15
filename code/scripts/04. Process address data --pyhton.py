# Databricks notebook source

import dlt
from pyspark.sql.functions import *


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
