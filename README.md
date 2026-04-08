# 🚀 Data Engineering Pipeline (Python + PostgreSQL)

Production-like ETL pipeline with incremental loading, data quality checks, metadata tracking, and historical data validation.

---

## 📌 Project Overview

This project demonstrates a full-cycle data pipeline built with a focus on real-world engineering practices:

**Source → RAW → Transform → STG → Quality → DWH → MART**

Key goal: build a reliable, observable, and scalable pipeline — not just a script.

---

## 🏗️ Architecture

- **Source**: Excel file
- **RAW layer**: file storage with change detection (SHA256)
- **Transform layer**: data cleaning and normalization (pandas)
- **STG (Staging)**: intermediate PostgreSQL table
- **DWH (Warehouse)**: cleaned and deduplicated data
- **MART**: aggregated business metrics
- **Metadata**: pipeline_runs table (run tracking)

---

## ⚙️ Key Features

### ✅ Incremental Loading (Watermark)
- Loads only new data using `InvoiceDate`
- Prevents reprocessing entire dataset

### 🔐 Deduplication
- Uses `row_hash` (SHA256)
- Protects against duplicate records

### 🧪 Data Quality Checks
- No NULLs in required fields
- No negative quantity or revenue
- No duplicate hashes
- Stops pipeline on failure

### 📊 Data Mart
- Daily aggregation:
  - revenue
  - number of orders
  - quantity

### 📈 Metadata & Observability
- `pipeline_runs` table tracks:
  - run_id
  - status (running/success/failed)
  - watermark
  - rows loaded
  - errors

### 🧠 Historical Data Control
- `historical_hash` detects changes in historical data
- If history changes → full reload is triggered

### 🧯 Error Handling
- Controlled errors (validation, source issues)
- Unexpected errors with full traceback

### ⚡ No-Op Runs
- If no new data → pipeline exits early

---

## 📂 Project Structure
de_pipeline_project/
│
├── data/
│ ├── source/
│ ├── raw/
│ └── processed/
│
├── pipeline/
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│ ├── quality.py
│ ├── metadata.py
│ ├── logger_config.py
│ ├── config.py
│ └── pipeline.py
│
└── README.md

---

## ▶️ How to Run

```bash
python -m pipeline.pipeline
⚙️ Configuration

Environment variables (stored in .env):

DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=

SOURCE_FILE=
RAW_DIR=
PROCESSED_DIR=

WAREHOUSE_TABLE=
MART_TABLE=

🧪 Example Data

Dataset: Online Retail (Excel)
~540k rows

🔄 Pipeline Logic
Check source file
Copy to RAW (if changed)
Read data
Apply incremental filter (watermark)
Clean and transform
Run quality checks
Load to STG
Load to DWH (deduplicated)
Build MART
Save metadata
⚠️ Edge Cases Covered
Missing source file
Corrupted Excel file
Empty dataset after cleaning
Duplicate rows
Historical data changes
No new data
🧠 What This Project Demonstrates
Real ETL architecture (not toy script)
Incremental processing
Data consistency handling
Error resilience
Observability (logs + metadata)
Clean modular design
🚀 Future Improvements
Airflow orchestration
Dockerization
Partitioning in DWH
Performance optimization (chunking / streaming)
API as data source
Unit tests for pipeline stages
👨‍💻 Author

Alexandr Parubets
Data Engineering Learner → Future AI Systems Engineer

⭐ If you like this project

Give it a star ⭐ and follow for updates
