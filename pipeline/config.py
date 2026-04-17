from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    # DB
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", 5432))
    db_name: str = os.getenv("DB_NAME", "de_db")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")

    # Logging
    log_file: str = os.getenv("LOG_FILE", "logs/pipeline.log")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    stg_table: str = os.getenv("STG_TABLE", "stg_orders")
    stg_schema: dict[str, str] = field(default_factory=lambda: {
    "invoiceno": "TEXT",
    "stockcode": "TEXT",
    "description": "TEXT",
    "quantity": "INTEGER",
    "invoicedate": "TIMESTAMP",
    "unitprice": "NUMERIC",
    "customerid": "INTEGER",
    "country": "TEXT",
    "revenue": "NUMERIC",
    "row_hash": "TEXT",
})
    raw_stg_table: str = os.getenv("RAW_STG_TABLE", "raw_stg_orders")
    raw_stg_schema: dict[str, str] = field(default_factory=lambda: {
    "invoiceno": "TEXT",
    "stockcode": "TEXT",
    "description": "TEXT",
    "quantity": "INTEGER",
    "invoicedate": "TIMESTAMP",
    "unitprice": "NUMERIC",
    "customerid": "INTEGER",
    "country": "TEXT",
})
    dwh_table: str = os.getenv("DWH_TABLE", "orders_clean")
    mart_table: str = os.getenv("MART_TABLE", "sales_daily")
    pipeline_runs_table: str = os.getenv("PIPELINE_RUNS_TABLE", "pipeline_runs")
    ml_table: str = os.getenv("ML_TABLE", "customer_ml_dataset")
    cf_table: str = os.getenv("CF_TABLE", "customer_features")
    c_scores: str = os.getenv("C_SCORES", "customer_scores")

    source_file: str = os.getenv(
    "SOURCE_FILE",
    "data/source/online_retail.xlsx",
)
    raw_dir: str = os.getenv("RAW_DIR", "data/raw")
    raw_file_prefix: str = os.getenv("RAW_FILE_PREFIX", "online_retail")

    chunk_size: int = int(os.getenv("CHUNK_SIZE", 5000))
    historical_period: str = os.getenv("HISTORICAL_PERIOD", "30 days")
    f_start: int = int(os.getenv("F_START", 60))
    f_end: int = int(os.getenv("F_END", 30))
    t_start: int = int(os.getenv("T_START", 30))
    t_end: int = int(os.getenv("T_END", 0))

    model_path: str = os.getenv("MODEL_PATH", "models/model.pkl")

    

settings = Settings()