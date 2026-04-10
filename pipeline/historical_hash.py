from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger
from pipeline.setup_db import truncate_dwh_table

logger = get_logger(__name__)



def get_new_boundary_date(engine):

    raw_stg_table = settings.raw_stg_table

    query = f"""
        SELECT MAX(invoicedate)
        FROM {raw_stg_table};
    """

    with engine.begin() as conn:
        result = conn.execute(text(query))
        new_boundary_date = result.scalar()

    if new_boundary_date is None:
        logger.warning("🧱 New boundary date is NULL (raw_stg is empty)")

    logger.info(f"🧱 New boundary date: {new_boundary_date}")

    return new_boundary_date


def get_historical_hash(engine, boundary_date, new=None):
    raw_stg_table = settings.raw_stg_table
    historical_period = settings.historical_period

    query = f"""
        SELECT md5(
            string_agg(
                md5(
                    concat_ws(
                        '||',
                        COALESCE(TRIM(invoiceno::text), ''),
                        COALESCE(TRIM(stockcode::text), ''),
                        COALESCE(TRIM(description::text), ''),
                        COALESCE(quantity::text, ''),
                        COALESCE(to_char(invoicedate, 'YYYY-MM-DD HH24:MI:SS'), ''),
                        COALESCE(unitprice::text, ''),
                        COALESCE(customerid::text, ''),
                        COALESCE(TRIM(country::text), '')
                    )
                ),
                '' ORDER BY
                md5(
                    concat_ws(
                        '||',
                        COALESCE(TRIM(invoiceno::text), ''),
                        COALESCE(TRIM(stockcode::text), ''),
                        COALESCE(TRIM(description::text), ''),
                        COALESCE(quantity::text, ''),
                        COALESCE(to_char(invoicedate, 'YYYY-MM-DD HH24:MI:SS'), ''),
                        COALESCE(unitprice::text, ''),
                        COALESCE(customerid::text, ''),
                        COALESCE(TRIM(country::text), '')
                    )
                )
            )
        )
        FROM {raw_stg_table}
        WHERE invoicedate < :boundary_date
          AND invoicedate >= :boundary_date - INTERVAL '{historical_period}';
    """

    with engine.begin() as conn:
        result = conn.execute(
            text(query),
            {"boundary_date": boundary_date},
        )
        historical_hash = result.scalar()
    if not new:
        logger.info(f"🧬 Historical hash: {historical_hash}")
    else:
        logger.info(f"🧬 New historical hash: {historical_hash}\n")

    return historical_hash


def check_historical_hash(engine, last_historical_hash, boundary_date, last_watermark):
    current_historical_hash = get_historical_hash(engine, boundary_date, None)

    # if this is the first run — just save and continue
    if last_historical_hash is None:
        logger.warning("⚠️ No previous historical hash found (first run)")
        truncate_dwh_table(engine)
        watermark = None

        return watermark

    if current_historical_hash != last_historical_hash:
        logger.warning(
            f"⚠️ HISTORICAL HASH CHANGED: {last_historical_hash} -> {current_historical_hash}."
        )
        logger.warning(f"⚠️ Truncating DWH and resetting watermark.")

        truncate_dwh_table(engine)
        watermark = None

        return watermark
    
    logger.info("♻️ Historical hash unchanged")
    watermark = last_watermark

    return watermark