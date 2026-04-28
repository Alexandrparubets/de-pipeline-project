from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_data_ml(engine) -> int:
    """
    Load aggregated data from DWH into ML table.

    Returns:
        int: Number of rows loaded into ML table.

    Raises:
        Exception: If ML load fails.
    """
    dwh_table = settings.dwh_table
    ml_table = settings.ml_table
    cf_table = settings.cf_table

    logger.info(
        f"🚀 Starting ML table load: source='{dwh_table}', target='{ml_table}'"
    )

    try:
        truncate_ml_table(engine, ml_table)
        insert_rows_to_ml(engine)
        ml_rows = get_ml_row_count(engine, ml_table)

        logger.info(
            f"✅ ML table load finished: table='{ml_table}', rows={ml_rows}\n"
        )

        return ml_rows

    except Exception as e:
        logger.exception(f"ML table load failed: {e}")
        raise


def truncate_ml_table(engine, table_name: str) -> None:
    """
    Truncate ML table before reload.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))

    logger.info(f"🧹 ML table truncated: '{table_name}'")


def insert_rows_to_ml(engine) -> None:
    """
    Aggregate data from DWH and insert into ML table.
    """
    
    insert_sql = f"""
        INSERT INTO {settings.ml_table} (
            customerid,
            orders_count_30,
            orders_count_7,
            total_spent_30,
            avg_order_30,
            unique_products_30,
            active_days_30,
            active_days_7,
            days_since_last_order,
            std_order_value,
            avg_days_between_orders,
            customer_lifetime_days,
            total_orders_count,
            total_spent_lifetime,
            avg_order_lifetime,
            order_frequency_ratio,
            target
        )
        WITH ref AS (
            SELECT MAX(invoicedate) - ({settings.f_end} * INTERVAL '1 day') AS ref_date
            FROM {settings.dwh_table}
        ),
        target_window AS (
            SELECT
                o.customerid,
                1 AS target
            FROM {settings.dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= (ref.ref_date + ({settings.t_end} * INTERVAL '1 day'))
            AND o.invoicedate <  (ref.ref_date + ({settings.t_start} * INTERVAL '1 day'))
            GROUP BY o.customerid
        )
        SELECT
            cf.customerid,
            cf.orders_count_30,
            cf.orders_count_7,
            cf.total_spent_30,
            cf.avg_order_30,
            cf.unique_products_30,
            cf.active_days_30,
            cf.active_days_7,
            cf.days_since_last_order,
            cf.std_order_value,
            cf.avg_days_between_orders,
            cf.customer_lifetime_days,
            cf.total_orders_count,
            cf.total_spent_lifetime,
            cf.avg_order_lifetime,
            cf.order_frequency_ratio,
            COALESCE(t.target, 0) AS target
        FROM {settings.cf_table} cf
        LEFT JOIN target_window t
            ON cf.customerid = t.customerid;
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))

    logger.info(f"📊 Data inserted into ML table '{settings.ml_table}'")


def get_ml_row_count(engine, table_name: str) -> int:
    """
    Get number of rows in ML table.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    return result