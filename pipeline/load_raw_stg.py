# pipeline/load_raw_stg.py

import pandas as pd

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_to_raw_stg(df: pd.DataFrame, engine) -> int:
    """
    Load  DataFrame into the RAW staging (RAW_STG) table.

    Args:
        df (pd.DataFrame): Cleaned DataFrame from transform step.
        engine: SQLAlchemy engine for database connection.

    Returns:
        int: Number of rows successfully loaded into RAW STG.

    Raises:
        ValueError: If the DataFrame is empty.
        Exception: If any error occurs during the load process.
    """
    if df.empty:
        logger.error("STG load skipped: DataFrame is empty.")
        raise ValueError("Cannot load empty DataFrame to RAW STG")

    logger.info(
        f"Starting RAW STG load: table='{settings.raw_stg_table}', rows={len(df)}"
    )

    try:
        df.to_sql(
            name=settings.raw_stg_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=settings.chunk_size,
        )

        loaded_rows = len(df)

        logger.info(
            f"RAW STG load finished: table='{settings.raw_stg_table}', loaded_rows={loaded_rows}"
        )

        return loaded_rows

    except Exception as e:
        logger.exception(
            f"RAW STG load failed: table='{settings.raw_stg_table}', error={e}"
        )
        raise


def align_to_raw_stg_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Align DataFrame columns to RAW STG schema.

    - missing RAW STG columns are added with None
    - extra DataFrame columns are ignored
    - final column order matches RAW STG schema
    """
    raw_stg_columns = list(settings.raw_stg_schema.keys())

    missing_cols = [col for col in raw_stg_columns if col not in df.columns]
    extra_cols = [col for col in df.columns if col not in raw_stg_columns]

    if missing_cols:
        logger.warning(f"Missing columns in DataFrame. They will be filled with NULLs: {missing_cols}")
        for col in missing_cols:
            df[col] = None

    if extra_cols:
        logger.warning(f"Extra columns in DataFrame. They will be skipped: {extra_cols}")

    df = df[raw_stg_columns].copy()

    return df