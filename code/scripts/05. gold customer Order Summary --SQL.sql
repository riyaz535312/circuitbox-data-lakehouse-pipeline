-- Databricks notebook source


CREATE OR REFRESH MATERIALIZED VIEW customer_order_summary
AS
SELECT 
        c.customer_id,
        c.customer_name,
        c.date_of_birth,
        c.telephone,
        c.email,
        a.address_line_1,
        a.city,
        a.state,
        a.postcode,
        COUNT(distinct o.order_id) as total_orders,
        SUM(o.item_quantity) as total_items_ordered,
        SUM(o.item_quantity * o.item_price) as total_order_amount,
        (SUM(o.item_quantity * o.item_price) / COUNT(distinct o.order_id)) as avg_order_value,
        MIN(CAST(o.order_timestamp AS DATE)) as first_order_date,
        MAX(CAST(o.order_timestamp AS DATE)) as last_order_date,
        DATEDIFF(MAX(CAST(o.order_timestamp AS DATE)), MIN(CAST(o.order_timestamp AS DATE))) as Customer_lifetime_days
        
FROM
   LIVE.sliver_customers c 
   JOIN LIVE.sliver_addresses a on c.customer_id=a.customer_id
   JOIN LIVE.sliver_orders o ON o.customer_id=c.customer_id
  WHERE a.__END_AT IS NULL
GROUP BY ALL;
