# 🧱 Medallion Architecture Data Warehouse (Bronze → Silver → Gold)

## 📖 Overview
This project implements a complete **Medallion Architecture** data pipeline using SQL (MySQL dialect).  
The design follows the industry-standard layered approach for modern data engineering, ensuring scalability, data quality, and analytical readiness.

### 🥉 Bronze Layer
The **Bronze layer** represents the raw ingestion zone.  
It stores data exactly as received from source systems with minimal transformations.

**Key Characteristics:**
- Schema mirrors the source systems (CRM, ERP, etc.).
- Used for data lineage and audit purposes.
- Acts as a single source of truth for raw data.

**Scripts:**
- `bronze_ddl.sql` → Creates all Bronze tables.
- `bronze_layer_load.sql` → Loads the data to the Bronze layer.

### 🥈 Silver Layer
The **Silver layer** performs data cleansing, standardization, and integration.  
It removes duplicates, validates formats, and unifies schema across sources.

**Highlights:**
- Tables prefixed with `silver_`
- Handles data standardization, referential consistency, and field normalization.
- Quality checks ensure no nulls, duplicates, or invalid dates.

**Scripts:**
- `silver_ddl.sql` → Creates all Silver tables.
- `silver_layer_load` → Loads the data to the Silver layer.
- `silver_quality_checks.sql` → Runs validation and data sanity tests.

### 🥇 Gold Layer
The **Gold layer** is the curated, analytics-ready zone.  
It defines **fact and dimension views** that model the data into a **Star Schema** for BI and reporting.

**Highlights:**
- Dimensions: `gold_dim_customers`, `gold_dim_products`
- Fact: `gold_fact_sales`
- Combines and enriches Silver data for business insights.

**Scripts:**
- `gold_views.sql` → Creates dimension and fact views.
- `gold_quality_checks.sql` → Ensures referential integrity and surrogate key uniqueness.
