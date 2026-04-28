# 🧠 ML Data Pipeline (End-to-End)

Production-like Data Engineering + ML pipeline:


SOURCE → RAW → STG → DWH → MART → ML → SCORING → DRIFT CHECK


---

## ⚙️ Tech Stack

- Python (pandas, scikit-learn)
- PostgreSQL
- SQLAlchemy
- joblib
- Logging (context-based)

---

## 🏗 Architecture

### 1. Data Pipeline


Excel → RAW → STG → DWH → MART


- **RAW** → snapshot + hash контроль
- **STG** → очистка (SQL)
- **DWH** → чистые данные (`orders_clean`)
- **MART** → агрегаты (`sales_daily`)

---

### 2. ML Pipeline


DWH → Feature Engineering → Train → Model Registry


- Feature windows (`f_start`, `f_end`)
- Target windows (`t_start`, `t_end`)
- Model: `GradientBoostingClassifier`
- Metric: ROC AUC

---

### 3. Model Registry (`ml_models`)

Хранит:

- model_path
- roc_auc
- threshold
- model_version
- is_active
- окна обучения

---

### 4. Scoring


Active Model → New Window → Predictions → Segments


Сегменты:

- high ≥ 0.7
- medium ≥ 0.4
- low < 0.4

---

### 5. Drift Detection


Baseline (train) vs Current (scoring)


Метрики:

- mean
- std
- median
- q25 / q75

Drift считается по % отклонению.

---

### 6. Tracking Tables

#### `ml_models`
Реестр моделей

#### `ml_model_baselines`
Baseline статистика по фичам

#### `customer_scores`
Результаты скоринга

#### `scoring_runs`
Метаданные скоринга:
- model_id
- rows_count
- drift_detected_mean
- drift_detected_std
- drift_threshold

---

## 📊 Logging

Пример:


🚀 ML scoring started
🪟 Scoring windows: f_start=80, f_end=50
📊 Segment ratio: {'low': 49%, 'medium': 31%, 'high': 20%}
📊 Drift summary:
🔴 days_since_last_order ...


---

## 🔄 Incremental Logic

- Watermark (InvoiceDate)
- Deduplication via `row_hash`
- `ON CONFLICT DO NOTHING`

---

## 🧪 Experiments

Поддержка:

- смещения окон (`train_shift`, `scoring_shift`)
- drift анализа
- сравнения моделей

---

## 🚀 How to Run

### 1. Train

```bash
python -m pipeline.run_train
2. Scoring
python -m pipeline.run_scoring
📁 Project Structure
pipeline/
├── run_train.py
├── run_scoring.py
├── train_model.py
├── score_model.py
├── build_drift_baseline.py
├── build_current_stats.py
├── load_ml_models.py
├── load_scoring_runs_table.py
└── ...
💡 Key Concepts
SQL-first transformations
Feature windows
Model registry
Drift detection
Production-like logging
📌 Status
✔ ETL pipeline
✔ ML training
✔ Scoring
✔ Drift detection
✔ Model registry
🔥 Next Steps
Retraining strategy (auto)
Monitoring dashboard
A/B testing моделей
