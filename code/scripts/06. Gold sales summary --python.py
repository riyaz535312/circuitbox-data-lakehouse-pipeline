# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Creating gold order summary
# MAGIC order_date
# MAGIC
# MAGIC total_orders
# MAGIC
# MAGIC total_revenue
# MAGIC
# MAGIC total_customers
# MAGIC
# MAGIC avg_order_value

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.materialized_view(
    name="gold_sales_summary",
    comment ="order summary for daily orders"
)
def gold_sales_summary():
    order_df=spark.read.table("sliver_orders").select("*",col("order_timestamp").cast("date").alias("order_date"))
    # or spark.read.table("sliver_orders").withColumn("order_date", col("order_timestamp").cast("date"))
    return (
        order_df.
        groupby("order_date")
        .agg(
            count("order_id").alias("total_orders"),
            sum(col("item_price") * col("item_quantity")).alias("total_revenue"),
            countDistinct("customer_id").alias("total_customers"),
        )
        .withColumn("average_order_value", col("total_revenue")/col("total_orders"))
    )