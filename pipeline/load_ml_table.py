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

    logger.info(
        f"🚀 Starting ML table load: source='{dwh_table}', target='{ml_table}'"
    )

    try:
        truncate_ml_table(engine, ml_table)
        insert_rows_to_ml(engine, dwh_table, ml_table)
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
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))

    logger.info(f"🧹 ML table truncated: '{table_name}'")


def insert_rows_to_ml(engine, dwh_table: str, ml_table: str) -> None:
    """
    Aggregate data from DWH and insert into ML table.
    """
    insert_sql = f"""
        INSERT INTO {ml_table} (
            customerid,
            orders_count,
            total_spent,
            avg_order,
            unique_products,
            active_days,
            target
        )
        WITH order_totals AS (
            SELECT
                customerid,
                invoiceno,
                SUM(revenue) AS order_value
            FROM {dwh_table}
            WHERE invoicedate >= CURRENT_DATE - ({settings.f_start} * INTERVAL '1 day')
            AND invoicedate <  CURRENT_DATE - ({settings.f_end} * INTERVAL '1 day')
            GROUP BY customerid, invoiceno
        ),
        order_features AS (
            SELECT
                customerid,
                COUNT(*)              AS orders_count_30d,
                SUM(order_value)      AS total_spent_30d,      
                AVG(order_value)      AS avg_order_value_30d
            FROM order_totals
            GROUP BY customerid
        ),
        txn_features AS (
            SELECT
                customerid,
                COUNT(DISTINCT stockcode)                  AS unique_products_30d,
                COUNT(DISTINCT DATE(invoicedate))          AS active_days_30d
            FROM {dwh_table}
            WHERE invoicedate >= CURRENT_DATE - ({settings.f_start} * INTERVAL '1 day')
            AND invoicedate <  CURRENT_DATE - ({settings.f_end} * INTERVAL '1 day')
            GROUP BY customerid
        ),
        customer_features AS (
            SELECT
                f.customerid,
                f.orders_count_30d,
                f.total_spent_30d,
                f.avg_order_value_30d,
                t.unique_products_30d,
                t.active_days_30d
            FROM order_features f
            JOIN txn_features t
            ON f.customerid = t.customerid
        ),
        target_table AS (
            SELECT
                customerid,
                1 AS target
            FROM {dwh_table}
            WHERE invoicedate >= CURRENT_DATE - ({settings.t_start} * INTERVAL '1 day')
            AND invoicedate <  CURRENT_DATE - ({settings.t_end} * INTERVAL '1 day')
            GROUP BY customerid
        )
        SELECT
            cf.customerid,
            cf.orders_count_30d,
            cf.total_spent_30d,
            cf.avg_order_value_30d,
            cf.unique_products_30d,
            cf.active_days_30d,
            COALESCE(t.target, 0) AS target
        FROM customer_features cf
        LEFT JOIN target_table t
        ON cf.customerid = t.customerid;
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))

    logger.info(f"📊 Data inserted into ML table '{ml_table}'")


def get_mart_row_count(engine, table_name: str) -> int:
    """
    Get number of rows in MART table.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    return result


def get_ml_row_count(engine, table_name: str) -> int:
    """
    Get number of rows in ML table.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    return result