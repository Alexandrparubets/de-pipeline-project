# 🚀 Retail Data Pipeline (Python + PostgreSQL)

## 📌 Overview

Production-like ETL pipeline for processing retail data.

Pipeline architecture:

```
source (Excel)
→ extract
→ RAW layer
→ transform (data cleaning)
→ incremental load (watermark)
→ PostgreSQL warehouse
→ data mart (aggregations)
```

---

## ⚙️ Features

* ✅ RAW data layer (immutable source of truth)
* ✅ Incremental loading using watermark (InvoiceDate)
* ✅ Data cleaning:

  * remove returns
  * remove nulls
  * remove duplicates
* ✅ Revenue calculation
* ✅ Idempotent pipeline (safe re-run)
* ✅ Logging (file + console)
* ✅ Config-driven (.env + config.py)
* ✅ Tail rebuild strategy for reliable incremental updates
* ✅ Data mart (`sales_daily`)

---

## 🏗️ Project Structure

```
de_pipeline_project/
│
├── pipeline/
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── logger_config.py
│   └── run_pipeline.py
│
├── data/
│   ├── source/
│   ├── raw/
│   └── processed/
│
├── logs/
├── .env
├── .gitignore
└── README.md
```

---

## 🔄 How It Works

### 1. Extract

* Reads source Excel file
* Detects changes (hash)
* Saves versioned copy to RAW layer

### 2. Transform

* Removes invalid data (returns, nulls)
* Removes duplicates
* Calculates revenue
* Applies incremental filter

### 3. Load

* Connects to PostgreSQL
* Uses incremental loading
* Rebuilds tail (`DELETE + INSERT`)
* Loads clean data into warehouse

### 4. Data Mart

* Aggregates daily sales (`sales_daily`)

---

## 🧠 Incremental Strategy

Pipeline uses:

* Watermark: `InvoiceDate`
* Filter: `>= last_loaded_date`
* Tail rebuild:

```
DELETE FROM warehouse WHERE InvoiceDate >= last_loaded_date
INSERT new cleaned data
```

This avoids:

* data loss on boundary timestamps
* duplicate loading
* inconsistent state

---

## ▶️ Run Pipeline

```bash
python -m pipeline.run_pipeline
```

---

## ⚙️ Configuration

All settings are stored in `.env`:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=de_db
DB_USER=postgres
DB_PASSWORD=...

SOURCE_FILE=online_retail.xlsx
CHUNK_SIZE=5000
```

---

## 📊 Tech Stack

* Python
* Pandas
* PostgreSQL
* SQLAlchemy
* Git

---

## 💡 Future Improvements

* [ ] DB-level deduplication (UNIQUE + ON CONFLICT)
* [ ] Airflow orchestration
* [ ] Dockerization
* [ ] Data validation framework
* [ ] Partitioned tables in PostgreSQL

---

## 👨‍💻 Author

Oleksandr Parubets

## 🔧 Update

Improved documentation and GitHub workflow practice.