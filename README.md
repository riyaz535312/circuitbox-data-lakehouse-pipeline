# circuitbox-data-lakehouse-pipeline
# CircuitBox Cross-Cloud Data Lakehouse Pipeline

A production-style data engineering project that simulates the data platform of an e-commerce company **CircuitBox**.

This pipeline ingests daily customer, order, and address data files from **AWS S3**, orchestrates workflows using **Apache Airflow**, transfers data to **Azure Data Lake Storage Gen2**, and triggers an **Azure Databricks Delta Live Tables (DLT)** pipeline implementing the **Medallion Architecture (Bronze → Silver → Gold)**.

The system automatically detects incremental files, archives processed files, triggers downstream processing, and sends email alerts when files are missing or contain no new data.

---

## Architecture Overview

AWS S3 <br>
⬇ <br>
Apache Airflow (Orchestration & File Validation) <br>
⬇ <br>
Azure Data Lake Storage Gen2 <br>
⬇ <br>
Azure Databricks Delta Live Tables <br>
⬇ <br>
Medallion Architecture <br>
• Bronze Layer<br>
• Silver Layer<br>
• Gold Layer<br>
⬇ <br>
Analytical Tables <br>
• Customer Summary<br>
• Order Summary<br>

---

## Tech Stack

| Technology                   | Purpose                                     |
| ---------------------------- | ------------------------------------------- |
| Apache Airflow               | Workflow orchestration                      |
| AWS S3                       | Source system for raw files                 |
| Azure Data Lake Storage Gen2 | Central data lake                           |
| Azure Databricks             | Data processing and transformation          |
| Delta Live Tables            | Automated ETL pipeline                      |
| Python                       | Pipeline logic and integrations             |
| SMTP                         | Email notifications for pipeline monitoring |

---

## Project Workflow

1. **Daily data files arrive in AWS S3**

   * Customer data
   * Order data
   * Address data

2. **Airflow DAG orchestration**

   * Checks for file availability
   * Validates incremental data
   * Sends email alerts if files are missing

3. **Cross-cloud data transfer**

   * Copies files from **AWS S3 → Azure ADLS Gen2**
   * Archives processed files in S3

4. **Trigger Databricks pipeline**

   * Airflow triggers **Databricks Delta Live Tables pipeline**

5. **Medallion data processing**

   * Bronze: Raw ingestion
   * Silver: Cleaned and transformed data
   * Gold: Business-ready aggregated tables

6. **Analytics layer**

   * Customer summary table
   * Order summary table

---

## Key Features

• Cross-cloud data pipeline (AWS → Azure)
• Automated orchestration using Apache Airflow
• Incremental file detection logic
• Data archiving for processed files
• Failure and missing file email notifications
• Lakehouse architecture using Databricks DLT
• Medallion data modeling (Bronze, Silver, Gold)

---

## Repository Structure

circuitbox-data-lakehouse-pipeline--root/

code/
    dags/
          circuitBox_pipeline_PROD.py
    scripts/
          setup.sql
          process Cutomer data.sql
          process order data.sql
          process address data.py
          gold customer summary.sql
          Gold sales summaer.py

   requirements.txt

data/
    addresses/
          day1.csv
          day2.csv
    customers/
          day1.json
          day2.json
    orders/
          day1.json
          day2.json
    

README.md

---

## Example Airflow DAG Workflow

check_s3_files<br>
⬇<br>
copy_s3_to_adls<br>
⬇<br>
trigger_databricks_dlt<br>
⬇<br>
email_notifications<br>

---

## Business Use Case

CircuitBox is an e-commerce platform that requires daily ingestion of operational data for analytics and reporting.

This pipeline enables:

• Automated ingestion of operational data
• Reliable cross-cloud data movement
• Scalable lakehouse architecture
• Analytical datasets for customer and order insights

---

## Sample Gold Tables

The Gold layer contains business-ready aggregated datasets used for analytics and reporting.

### Customer Summary (Gold)

Provides a consolidated view of each customer's lifetime activity including order behavior and revenue contribution.

| customer_id | customer_name | date_of_birth | telephone     | email                                                   | address_line_1           | city           | state     | postcode | total_orders | total_items_ordered | total_order_amount | avg_order_value | first_order_date | last_order_date | customer_lifetime_days |
| ----------- | ------------- | ------------- | ------------- | ------------------------------------------------------- | ------------------------ | -------------- | --------- | -------- | ------------ | ------------------- | ------------------ | --------------- | ---------------- | --------------- | ---------------------- |
| 1002        | Carla Morton  | 2004-06-21    | +1 8616454195 | [carla.morton@yahoo.com](mailto:carla.morton@yahoo.com) | 084 Anne Hollow Apt. 064 | East Jasontown | Minnesota | 77329    | 1            | 3                   | 897                | 897             | 2024-10-13       | 2024-10-13      | 0                      |

This table enables:
• Customer lifetime value analysis
• Customer segmentation
• Purchase behavior insights

---

### Order Summary (Gold)

Aggregated order-level metrics used for daily business reporting and performance tracking.

| order_date | total_orders | total_revenue | total_customers | average_order_value |
| ---------- | ------------ | ------------- | --------------- | ------------------- |
| 2024-10-25 | 3            | 1697          | 2               | 565.66   |

This table enables:
• Daily revenue monitoring
• Order volume tracking
• Average order value analysis
• Customer purchasing trends

---

## How to Run the Project

1. Start Apache Airflow (Docker or local setup)

2. Configure connections

   * AWS S3 credentials
   * Azure Storage credentials
   * Databricks workspace
   * smtp connection

3. Place DAG inside the `dags` directory

4. Trigger the DAG from Airflow UI

5. Monitor pipeline execution and logs


---

## Author

Riyaz Shaik <br>
Data Engineering Project
