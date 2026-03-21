from pathlib import Path
from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


BASE_DIR = Path(__file__).resolve().parents[1]


@dataclass
class Settings:
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", 5432))
    db_name: str = os.getenv("DB_NAME", "de_db")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres123")

    source_file: str = os.getenv("SOURCE_FILE", "online_retail.xlsx")
    raw_file_prefix: str = os.getenv("RAW_FILE_PREFIX", "online_retail")
    log_file: str = os.getenv("LOG_FILE", "pipeline.log")

    source_dir: Path = BASE_DIR / os.getenv("SOURCE_DIR", "data/source")
    raw_dir: Path = BASE_DIR / os.getenv("RAW_DIR", "data/raw")
    processed_dir: Path = BASE_DIR / os.getenv("PROCESSED_DIR", "data/processed")
    log_dir: Path = BASE_DIR / os.getenv("LOG_DIR", "logs")

    warehouse_table: str = os.getenv("WAREHOUSE_TABLE", "orders_clean")
    mart_table: str = os.getenv("MART_TABLE", "sales_daily")

    chunk_size: int = int(os.getenv("CHUNK_SIZE", 5000))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()

def ensure_directories():
    settings.source_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    settings.processed_dir.mkdir(parents=True, exist_ok=True)
    #settings.log_dir.mkdir(parents=True, exist_ok=True)# - перенес в logger_config.py