from sqlalchemy import text
import pandas as pd
from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def build_ml_dataset_df(
    engine,
    dwh_table: str,
    f_start: int,
    f_end: int,
    t_start: int,
    t_end: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:

    query = f"""
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
            WHERE invoicedate >= (ref.max_date - INTERVAL '1 day' * :f_start)
            AND invoicedate <  (ref.max_date - INTERVAL '1 day' * :f_end)
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
            WHERE o.invoicedate >= (ref.max_date - INTERVAL '1 day' * :f_start)
            AND o.invoicedate <  (ref.max_date - INTERVAL '1 day' * :f_end)
            GROUP BY o.customerid
        ),
        customer_last_order AS (
            SELECT
                lod.customerid,
                lod.last_order_date,
                DATE_PART(
                    'day',
                    (ref.max_date - INTERVAL '1 day' * :f_start) - lod.last_order_date
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
            WHERE o.invoicedate >= (ref.max_date - INTERVAL '1 day' * :f_start)
            AND o.invoicedate <  (ref.max_date - INTERVAL '1 day' * :f_end)
            GROUP BY o.customerid
        ),
        orders_count_7 AS (
            SELECT
                o.customerid,
                COUNT(DISTINCT o.invoiceno) AS orders_count_7d
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= ((ref.max_date - INTERVAL '1 day' * :f_end) - INTERVAL '7 days')
            AND o.invoicedate <  (ref.max_date - INTERVAL '1 day' * :f_end)
            GROUP BY o.customerid
        ),
        customer_active_7 AS (
            SELECT
                o.customerid,
                COUNT(DISTINCT DATE(o.invoicedate)) AS active_days_7d
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= ((ref.max_date - INTERVAL '1 day' * :f_end) - INTERVAL '7 days')
            AND o.invoicedate <  (ref.max_date - INTERVAL '1 day' * :f_end)
            GROUP BY o.customerid
        ),
        customer_orders AS (
            SELECT
                o.customerid,
                o.invoiceno,
                MIN(o.invoicedate) AS order_date
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= (ref.max_date - INTERVAL '1 day' * :f_start)
            AND o.invoicedate <  (ref.max_date - INTERVAL '1 day' * :f_end)
            GROUP BY o.customerid, o.invoiceno
        ),
        order_gaps AS (
            SELECT
                customerid,
                order_date,
                LAG(order_date) OVER (
                    PARTITION BY customerid
                    ORDER BY order_date
                ) AS prev_order_date
            FROM customer_orders
        ),
        customer_avg_days_between_orders AS (
            SELECT
                customerid,
                COALESCE(AVG(DATE_PART('day', order_date - prev_order_date)), 0) AS avg_days_between_orders
            FROM order_gaps
            WHERE prev_order_date IS NOT NULL
            GROUP BY customerid
        ),
        customer_lifetime AS (
            SELECT
                o.customerid,
                DATE_PART(
                    'day',
                    (ref.max_date - INTERVAL '1 day' * :f_end) - MIN(o.invoicedate)
                )::INTEGER AS customer_lifetime_days,
                COUNT(DISTINCT o.invoiceno) AS total_orders_count
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate < (ref.max_date - INTERVAL '1 day' * :f_end)
            GROUP BY o.customerid, ref.max_date
        ),
        lifetime_order_totals AS (
            SELECT
                o.customerid,
                o.invoiceno,
                SUM(o.revenue) AS order_value
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate < (ref.max_date - INTERVAL '1 day' * :f_end)
            GROUP BY o.customerid, o.invoiceno
        ),
        customer_lifetime_order_stats AS (
            SELECT
                customerid,
                SUM(order_value) AS total_spent_lifetime,
                AVG(order_value) AS avg_order_lifetime
            FROM lifetime_order_totals
            GROUP BY customerid
        ),
        target AS (
            SELECT
                o.customerid,
                1 AS target
            FROM {dwh_table} o
            CROSS JOIN ref
            WHERE o.invoicedate >= (ref.max_date - INTERVAL '1 day' * :t_start)
            AND o.invoicedate <  (ref.max_date - INTERVAL '1 day' * :t_end)
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
                COALESCE(cos.std_order_value, 0)::NUMERIC(14,2) AS std_order_value,
                COALESCE(cog.avg_days_between_orders, 0) AS avg_days_between_orders,
                COALESCE(cl.customer_lifetime_days, 0) AS customer_lifetime_days,
                COALESCE(cl.total_orders_count, 0) AS total_orders_count,
                COALESCE(cols.total_spent_lifetime, 0) AS total_spent_lifetime,
                COALESCE(cols.avg_order_lifetime, 0) AS avg_order_lifetime,
                CASE 
                    WHEN of.orders_count_30d = 0 THEN 0
                    ELSE COALESCE(o7.orders_count_7d, 0)::float / of.orders_count_30d
                END AS order_frequency_ratio,
                COALESCE(t.target, 0) AS target
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
                ON of.customerid = cos.customerid
            LEFT JOIN customer_avg_days_between_orders cog
                ON of.customerid = cog.customerid
            LEFT JOIN customer_lifetime cl
                ON of.customerid = cl.customerid
            LEFT JOIN customer_lifetime_order_stats cols
                ON of.customerid = cols.customerid
            LEFT JOIN target t
                ON of.customerid = t.customerid;
        """
    params = {
        "f_start": f_start,
        "f_end": f_end,
        "t_start": t_start,
        "t_end": t_end,
    }
    df = pd.read_sql(text(query), engine, params=params)

    feature_cols = [
        "total_spent_30",
        "avg_order_30",
        "unique_products_30",
        "days_since_last_order",
        "customer_lifetime_days",
        "total_orders_count"
    ]

    if df.empty:
        raise ValueError("ML dataset is empty")

    X = df[feature_cols]
    y = df["target"]

    logger.info(
    f"📦 ML dataset built: rows={len(df)}, cols={len(df.columns)}, "
    f"features_shape={X.shape}, target_shape={y.shape}"
    )
    logger.info(f"🧩 Feature columns: {feature_cols}")
    target_distribution = y.value_counts(dropna=False).to_dict()
    logger.info(f"🎯 Target distribution: {target_distribution}\n")

    return df, X, y
