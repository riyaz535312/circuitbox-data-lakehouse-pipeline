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

project-root/

dags/
    s3_to_adls_pipeline.py

scripts/
    copy_s3_to_adls.py

databricks/
    dlt_pipeline.py

docs/
    dag_graph.png
    architecture.png

requirements.txt

README.md

---

## Example Airflow DAG Workflow

check_s3_files<br>
⬇<br>
validate_incremental_files<br>
⬇<br>
copy_s3_to_adls<br>
⬇<br>
archive_s3_files<br>
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

### Customer Summary

| Customer ID | Total Orders | Total Spend |
| ----------- | ------------ | ----------- |

Provides a consolidated view of customer activity.

### Order Summary

| Order Date | Total Orders | Revenue |
| ---------- | ------------ | ------- |

Provides aggregated metrics for business analytics.

---

## How to Run the Project

1. Start Apache Airflow (Docker or local setup)

2. Configure connections

   * AWS S3 credentials
   * Azure Storage credentials
   * Databricks workspace

3. Place DAG inside the `dags` directory

4. Trigger the DAG from Airflow UI

5. Monitor pipeline execution and logs

---

## Future Improvements

• Add data quality checks using Great Expectations
• Implement CI/CD for Airflow DAG deployment
• Add monitoring dashboards
• Implement schema validation for incoming files

---

## Author

Riyaz
Data Engineering Project
