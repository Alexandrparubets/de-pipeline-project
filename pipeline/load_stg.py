from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_raw_stg_to_stg(engine) -> dict:
    """
    Load data from RAW STG to STG.

    Returns:
        dict: Load statistics with inserted rows.

    Raises:
        Exception: If load to STG fails.
    """
    raw_stg_table = settings.raw_stg_table
    stg_table = settings.stg_table

    logger.info(
        f"Starting STG load: source='{raw_stg_table}', target='{stg_table}'"
    )

    try:
        
        inserted_rows = insert_all_rows_to_stg(engine, raw_stg_table, stg_table)
        

        logger.info(
            f"STG load finished: inserted_rows={inserted_rows}, "
        )

        return {           
            "inserted_rows": inserted_rows,
        }

    except Exception as e:
        logger.exception(f"STG load failed: {e}")
        raise



def insert_all_rows_to_stg(engine, raw_stg_table: str, stg_table: str) -> int:
    """
    Insert all rows from RAW STG into STG, skipping duplicates.

    Returns:
        int: Number of rows actually inserted.
    """
    raw_columns = list(settings.raw_stg_schema.keys())
    raw_columns_sql = ", ".join(raw_columns)
    columns = list(settings.stg_schema.keys())
    columns_sql = ", ".join(columns)

    insert_sql = f"""
    INSERT INTO {stg_table} (
        invoiceno,
        stockcode,
        description,
        quantity,
        invoicedate,
        unitprice,
        customerid,
        country,
        revenue,
        row_hash
    )
    SELECT 
        invoiceno,
        stockcode,
        description,
        quantity,
        invoicedate,
        unitprice,
        customerid,
        country,
        quantity * unitprice AS revenue,
        MD5(
            COALESCE(invoiceno, '') ||
            COALESCE(stockcode, '') ||
            COALESCE(description, '') ||
            COALESCE(quantity::text, '') ||
            COALESCE(invoicedate::text, '') ||
            COALESCE(unitprice::text, '') ||
            COALESCE(customerid::text, '') ||
            COALESCE(country, '')
        ) AS row_hash
    FROM {raw_stg_table}
        WHERE invoiceno NOT LIKE 'C%'
        AND quantity > 0
        AND customerid IS NOT NULL
        AND quantity * unitprice > 0
    RETURNING 1
    ;
"""

    with engine.begin() as conn:
        result = conn.execute(text(insert_sql))
        inserted_rows = len(result.fetchall())
        raw_count = conn.execute(
        text(f"SELECT COUNT(*) FROM {raw_stg_table}")
        ).scalar()

    filtered_out = raw_count - inserted_rows

    logger.info(
    f"STG transform: raw_rows={raw_count}, inserted={inserted_rows}, filtered_out={filtered_out}"
)

    return inserted_rows