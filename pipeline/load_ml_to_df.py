import pandas as pd
from sqlalchemy import text
from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_ml_dataset(engine):
    """
    Loads an ML dataset from a table and splits it into X (features) and y (target)
    """

    table_name = settings.ml_table
    logger.info(f"📥 Loading ML dataset from table '{table_name}'")

    query = f"""
    SELECT
        customerid,
        orders_count,
        total_spent,
        avg_order,
        unique_products,
        active_days,
        target
    FROM {table_name};
    """

    with engine.begin() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        logger.error("❌ ML dataset is empty")
        raise ValueError("ML dataset is empty")
    
    logger.info(f"📊 Rows loaded: {len(df)}")

    # Features
    X = df[
        [
            "orders_count",
            "total_spent",
            "avg_order",
            "unique_products",
            "active_days",
        ]
    ].copy()

    # Target
    y = df["target"].copy()

    logger.info(f"✅ ML dataset prepared: X_shape={X.shape}, y_shape={y.shape}\n")


    return X, y