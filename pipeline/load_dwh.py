from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_stg_to_dwh(engine) -> dict:
    """
    Load validated data from STG to DWH.

    Returns:
        dict: Load statistics with attempted, inserted, and skipped rows.

    Raises:
        Exception: If load to DWH fails.
    """
    stg_table = settings.stg_table
    dwh_table = settings.dwh_table

    logger.info(
        f"Starting DWH load: source='{stg_table}', target='{dwh_table}'"
    )

    try:
        attempted_rows = get_stg_row_count(engine, stg_table)
        inserted_rows = insert_new_rows_to_dwh(engine, stg_table, dwh_table)
        skipped_rows = attempted_rows - inserted_rows

        logger.info(
            f"DWH load finished: attempted={attempted_rows}, "
            f"inserted={inserted_rows}, skipped={skipped_rows}"
        )

        return {
            "attempted_rows": attempted_rows,
            "inserted_rows": inserted_rows,
            "skipped_rows": skipped_rows,
        }

    except Exception as e:
        logger.exception(f"DWH load failed: {e}")
        raise


def get_stg_row_count(engine, table_name: str) -> int:
    """
    Get total number of rows in STG table.

    Returns:
        int: Number of rows in STG.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    logger.info(f"STG row count: {result}")

    return result


def insert_new_rows_to_dwh(engine, stg_table: str, dwh_table: str) -> int:
    """
    Insert new rows from STG into DWH, skipping duplicates.

    Returns:
        int: Number of rows actually inserted.
    """
    columns = list(settings.stg_schema.keys())
    columns_sql = ", ".join(columns)

    insert_sql = f"""
        INSERT INTO {dwh_table} ({columns_sql})
        SELECT {columns_sql}
        FROM {stg_table}
        ON CONFLICT (row_hash) DO NOTHING
        RETURNING 1;
    """

    with engine.begin() as conn:
        result = conn.execute(text(insert_sql))
        inserted_rows = len(result.fetchall())

    logger.info(f"Inserted rows into DWH: {inserted_rows}")

    return inserted_rows