from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_raw_stg_to_stg(engine, last_watermark) -> dict:
    """
    Load data from RAW STG to STG.

    """
    raw_stg_table = settings.raw_stg_table
    stg_table = settings.stg_table

    logger.info(f"🚀 Starting STG load: source='{raw_stg_table}', target='{stg_table}'")
    logger.info(f"📅 Using watermark: {last_watermark}")

    try:
        
        inserted_rows, raw_count = insert_all_rows_to_stg(engine, last_watermark, raw_stg_table, stg_table)
    
        logger.info(f"📊 Rows in RAW_STG: {raw_count}")
        
        rows_after_watermark = get_rows_after_watermark(engine, last_watermark)
        filtered_out=raw_count - rows_after_watermark

        logger.info(f"📅 Rows after watermark: {rows_after_watermark}, filtered out: {filtered_out}")

        skipped = rows_after_watermark - inserted_rows

        logger.info(f"🗑️ Business filters applied: {skipped}")

        logger.info(f"📦 Rows ready for DWH load: {inserted_rows}")

        return inserted_rows

    except Exception as e:
        logger.exception(f"STG load failed: {e}")
        raise



def insert_all_rows_to_stg(engine, last_watermark, raw_stg_table: str, stg_table: str) -> int:
    """
    Insert all rows from RAW STG into STG, skipping duplicates.

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
        COALESCE(UPPER(TRIM(REGEXP_REPLACE(invoiceno, '\\s+', ' ', 'g'))),'') AS invoiceno,
        COALESCE(UPPER(TRIM(REGEXP_REPLACE(stockcode, '\\s+', ' ', 'g'))),'') AS stockcode,
        COALESCE(UPPER(TRIM(REGEXP_REPLACE(description, '\\s+', ' ', 'g'))),'') AS description,
        COALESCE((quantity::INTEGER),0) AS quantity,
        invoicedate::TIMESTAMP AS invoicedate,
        COALESCE((unitprice::NUMERIC),0) AS unitprice,
        customerid::INTEGER AS customerid,
        COALESCE(UPPER(TRIM(REGEXP_REPLACE(country, '\\s+', ' ', 'g'))),'') AS country,
        quantity * unitprice AS revenue,
        MD5(
            COALESCE(UPPER(TRIM(REGEXP_REPLACE(invoiceno, '\\s+', ' ', 'g'))), '') ||
            COALESCE(UPPER(TRIM(REGEXP_REPLACE(stockcode, '\\s+', ' ', 'g'))), '') ||
            COALESCE(UPPER(TRIM(REGEXP_REPLACE(description, '\\s+', ' ', 'g'))), '') ||
            COALESCE(quantity::text, '') ||
            COALESCE(TO_CHAR(invoicedate, 'YYYY-MM-DD HH24:MI'), '') ||
            COALESCE(unitprice::text, '') ||
            COALESCE(customerid::text, '') ||
            COALESCE(UPPER(TRIM(REGEXP_REPLACE(country, '\\s+', ' ', 'g'))), '')
        ) AS row_hash
    FROM {raw_stg_table}
        WHERE invoiceno NOT LIKE 'C%'
        AND quantity > 0
        AND customerid IS NOT NULL
        AND quantity * unitprice > 0
        AND (
        :last_watermark IS NULL
        OR invoicedate >= :last_watermark
         )
        ON CONFLICT (row_hash) DO NOTHING
    RETURNING 1
    ;
    """

    with engine.begin() as conn:
        result = conn.execute(text(insert_sql), {"last_watermark": last_watermark})
        inserted_rows = len(result.fetchall())

        raw_count = conn.execute(
        text(f"SELECT COUNT(*) FROM {raw_stg_table}")
        ).scalar()
      
    return inserted_rows, raw_count


def get_last_watermark_value(engine):

    stg_table = settings.stg_table

    query = f"""
        SELECT MAX(invoicedate)
        FROM {stg_table};
    """

    with engine.begin() as conn:
        result = conn.execute(text(query))
        watermark = result.scalar()

    logger.info(f"🆕 New watermark: {watermark}\n")

    return watermark


def get_rows_after_watermark(engine, last_watermark):

    raw_stg_table = settings.raw_stg_table

    if last_watermark is None:
        query = f"SELECT COUNT(*) FROM {raw_stg_table};"
        params = {}
    else:
        query = f"""
        SELECT COUNT(*)
        FROM {raw_stg_table}
        WHERE invoicedate >= :last_watermark;
    """
        params = {"last_watermark": last_watermark}

    with engine.begin() as conn:
        result = conn.execute(
            text(query),
            params,
        )
        rows_after_watermark = result.scalar()
    
    return rows_after_watermark





