# PostgreSQL Data Warehouse (Medallion Architecture)

A custom-built Data Warehouse deployed via Docker on a Linux server, processing raw ERP and CRM data from CSV files. This project implements a full Medallion Architecture (Bronze, Silver, Gold) using custom PostgreSQL stored procedures to handle the ETL pipelines, data cleaning, and automated logging.

* **Bronze Layer:** Raw data ingestion. Data is loaded directly from CSV files into PostgreSQL using the `COPY` command. Tables are truncated and reloaded during each batch run.
* **Silver Layer:** Data cleansing and transformation. Includes standardizing formats, handling missing values, and joining disparate CRM and ERP sources.
* **Gold Layer:** Business-level aggregations and dimensional modeling (Star Schema) ready for BI tools and analysis.