import joblib
import pandas as pd
import numpy as np
from sqlalchemy import text
from pipeline.logger_config import get_logger
from pipeline.config import settings
import os

logger = get_logger(__name__)


def save_model(model):
    logger.info(f"💾 Saving model to {settings.model_path}")
    os.makedirs(os.path.dirname(settings.model_path), exist_ok=True)
    joblib.dump(model, settings.model_path)

    logger.info("✅ Model saved successfully")


def score_model(model, X):

    logger.info("🎯 Starting model scoring")

    logger.info(f"📦 Loading model from {settings.model_path}")
    model = joblib.load(settings.model_path)

    logger.info(f"📊 Scoring data shape: {X.shape}")
    y_prob = model.predict_proba(X)[:, 1]

    logger.info(f"📈 Avg probability: {y_prob.mean():.4f}")
    logger.info("✅ Scoring completed")

    return y_prob


def model_to_db(df, X, y_prob):

    logger.info("📤 Preparing data for DB insert")

    if len(X) != len(y_prob):
        raise ValueError("Length mismatch between X and y_prob")

    logger.info(f"📊 Input sizes → X: {len(X)}, y_prob: {len(y_prob)}")

    df_result = df.loc[X.index].copy()

    df_result["probability"] = np.array(y_prob)

    df_result = df_result[["customerid", "probability"]]

    logger.info(f"📦 Prepared rows: {len(df_result)}\n")

    return df_result


def insert_scores(engine, df_result, table_name: str) -> None:

    logger.info("📥 Starting insert into scores table")

    if df_result.empty:
        logger.warning("⚠️ No data to insert into scores table")
        return
    
    insert_sql = f"""
        INSERT INTO {table_name} (customerid, probability)
        VALUES (:customerid, :probability)
    """

    data = df_result.to_dict(orient="records")

    logger.info(f"📦 Rows to insert: {len(df_result)}")
    logger.info(f"🗂 Target table: {table_name}")

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        conn.execute(text(insert_sql), data)

    logger.info("✅ Insert completed\n")
    
