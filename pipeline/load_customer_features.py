from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_cf_table(engine) -> int:
    """
    Load aggregated data from DWH into CF table.

    Returns:
        int: Number of rows loaded into ML table.

    Raises:
        Exception: If CF load fails.
    """
    dwh_table = settings.dwh_table
    cf_table = settings.cf_table

    logger.info(
        f"🚀 Starting ML table load: source='{dwh_table}', target='{cf_table}'"
    )

    try:
        truncate_cf_table(engine, cf_table)
        insert_rows_to_cf_table(engine, dwh_table, cf_table)
        cf_rows = get_cf_table_row_count(engine, cf_table)

        logger.info(
            f"✅ CF table load finished: table='{cf_table}', rows={cf_rows}\n"
        )

        return cf_rows

    except Exception as e:
        logger.exception(f"CF table load failed: {e}")
        raise


def truncate_cf_table(engine, table_name: str) -> None:
    """
    Truncate CF table before reload.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))

    logger.info(f"🧹 CF table truncated: '{table_name}'")


def insert_rows_to_cf_table(engine, dwh_table: str, cf_table: str) -> None:
    """
    Aggregate data from DWH and insert into CF table.
    """
    insert_sql = f"""
        INSERT INTO {settings.cf_table} (
            customerid,
            orders_count_30,
            orders_count_7,
            total_spent_30,
            avg_order_30,
            unique_products_30,
            active_days_30,
            active_days_7,
            days_since_last_order,
            std_order_value
        )
        WITH ref AS (
            SELECT MAX(invoicedate) AS max_date
            FROM {dwh_table}
        ),
        order_totals AS (
            SELECT
                customerid,
                invoiceno,
                SUM(revenue) AS order_value
            FROM {dwh_table}
            CROSS JOIN ref
            WHERE invoicedate >= (ref.max_date - ({settings.f_start} * INTERVAL '1 day'))
            AND invoicedate <  (ref.max_date - ({settings.f_end} * INTERVAL '1 day'))
            GROUP BY customerid, invoiceno
        ),
        customer_order_std AS (
            SELECT
                customerid,
                STDDEV(order_value) AS std_order_value
            FROM order_totals
            GROUP BY customerid
        ),
        last_order_dates AS (
            SELECT
                o.customerid,
                MAX(o.invoicedate) AS last_order_date
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= (ref.max_date - ({settings.f_start} * INTERVAL '1 day'))
            AND o.invoicedate <  (ref.max_date - ({settings.f_end} * INTERVAL '1 day'))
            GROUP BY o.customerid
        ),
        customer_last_order AS (
            SELECT
                lod.customerid,
                lod.last_order_date,
                DATE_PART(
                    'day',
                    (ref.max_date - ({settings.f_end} * INTERVAL '1 day')) - lod.last_order_date
                )::INTEGER AS days_since_last_order
            FROM last_order_dates lod
            CROSS JOIN ref
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
                o.customerid,
                COUNT(DISTINCT o.stockcode)           AS unique_products_30d,
                COUNT(DISTINCT DATE(o.invoicedate))   AS active_days_30d
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= (ref.max_date - ({settings.f_start} * INTERVAL '1 day'))
            AND o.invoicedate <  (ref.max_date - ({settings.f_end} * INTERVAL '1 day'))
            GROUP BY o.customerid
        ),
        orders_count_7 AS (
            SELECT
                o.customerid,
                COUNT(DISTINCT o.invoiceno) AS orders_count_7d
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= (ref.max_date - ({settings.f_end} * INTERVAL '1 day') - INTERVAL '7 days')
            AND o.invoicedate <  (ref.max_date - ({settings.f_end} * INTERVAL '1 day'))
            GROUP BY o.customerid
        ),
        customer_active_7 AS (
            SELECT
                o.customerid,
                COUNT(DISTINCT DATE(o.invoicedate)) AS active_days_7d
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= (ref.max_date - ({settings.f_end} * INTERVAL '1 day') - INTERVAL '7 days')
            AND o.invoicedate <  (ref.max_date - ({settings.f_end} * INTERVAL '1 day'))
            GROUP BY o.customerid
        )
            SELECT
                of.customerid,
                of.orders_count_30d                AS orders_count_30,
                COALESCE(o7.orders_count_7d, 0)    AS orders_count_7,
                of.total_spent_30d                 AS total_spent_30,
                of.avg_order_value_30d             AS avg_order_30,
                tf.unique_products_30d             AS unique_products_30,
                tf.active_days_30d                 AS active_days_30,
                COALESCE(ca7.active_days_7d, 0)    AS active_days_7,
                clo.days_since_last_order          AS days_since_last_order,
                COALESCE(cos.std_order_value, 0)::NUMERIC(14,2) AS std_order_value
            FROM order_features of
            LEFT JOIN txn_features tf
                ON of.customerid = tf.customerid
            LEFT JOIN customer_active_7 ca7
                ON of.customerid = ca7.customerid
            LEFT JOIN customer_last_order clo
                ON of.customerid = clo.customerid
            LEFT JOIN orders_count_7 o7
                ON of.customerid = o7.customerid
            LEFT JOIN customer_order_std cos
                ON of.customerid = cos.customerid;
        """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))

    logger.info(f"📊 Data inserted into CF table '{cf_table}'")


def get_cf_table_row_count(engine, table_name: str) -> int:
    """
    Get number of rows in CF table.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()

    return result


