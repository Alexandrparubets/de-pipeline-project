import joblib
import pandas as pd
import numpy as np
from sqlalchemy import text
from pipeline.logger_config import get_logger
from pipeline.config import settings
import os

logger = get_logger(__name__)


def score_model(X, model_path):

    logger.info("\n--------- MODEL SCORING ---------")
    logger.info("🎯 Starting model scoring")

    logger.info(f"📦 Loading model from {settings.model_path}")
   
    try:
        model = joblib.load(model_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Model file not found: {model_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load model: {e}")

    logger.info(f"📦 Model loaded from {model_path}")
    logger.info(f"🧠 Model features: {list(model.feature_names_in_)}")
    logger.info(f"📥 Input features: {list(X.columns)}")

    # 1. Check that model has feature_names_in_
    if not hasattr(model, "feature_names_in_"):
        raise ValueError("Model has no feature_names_in_")

    model_features = list(model.feature_names_in_)
    input_features = list(X.columns)

    # 2. Check for missing columns
    missing_cols = set(model_features) - set(input_features)
    if missing_cols:
        raise ValueError(f"Missing columns for scoring: {missing_cols}")

    # 3. (optional) handle extra columns
    extra_cols = set(input_features) - set(model_features)
    if extra_cols:
        logger.warning(f"⚠️ Extra columns will be ignored: {extra_cols}")

    # 4. Align column order with the model
    X = X[model_features]

    logger.info(f"✅ Features aligned for scoring: {model_features}")

    logger.info(f"📊 Scoring data shape: {X.shape}")
    y_prob = model.predict_proba(X)[:, 1]

    logger.info(
    f"📈 Probability stats: min={y_prob.min():.4f}, "
    f"max={y_prob.max():.4f}, avg={y_prob.mean():.4f}"
    )
    logger.info("✅ Scoring completed")

    return y_prob


def model_to_db(df, X, y_prob, threshold, model_id):

    logger.info("\n--------- PREPARE FOR DB ---------")
    logger.info("📤 Preparing data for DB insert")

    if len(X) != len(y_prob):
        raise ValueError("Length mismatch between X and y_prob")

    logger.info(f"📊 Input sizes → X: {len(X)}, y_prob: {len(y_prob)}")

    if not (0 <= threshold <= 1):
        raise ValueError(f"Invalid THRESHOLD: {threshold}")

    df_result = df.loc[X.index].copy()

    df_result["model_id"] = model_id

    df_result["probability"] = np.array(y_prob)

    df_result["prediction"] = (df_result["probability"] >= threshold).astype(int)

    df_result = df_result[["customerid", "model_id", "probability", "prediction"]]

    logger.info(f"🎯 Using threshold: {threshold}")
    logger.info(f"📊 Predictions distribution: {df_result['prediction'].value_counts().to_dict()}")

    logger.info(f"📦 Prepared rows for DB insert: {len(df_result)}")
    logger.info(f"🧩 Output columns: {list(df_result.columns)}\n")

    return df_result


def insert_scores(engine, df_result, table_name: str) -> None:
    logger.info("\n--------- DB INSERT ---------")
    logger.info("📥 Starting insert into scores table")

    if df_result.empty:
        logger.warning("⚠️ No data to insert into scores table")
        return
    
    insert_sql = f"""
        INSERT INTO {table_name} (customerid, model_id, probability, prediction)
        VALUES (:customerid, :model_id, :probability, :prediction)
    """

    data = df_result.to_dict(orient="records")

    logger.info(f"📦 Rows to insert: {len(df_result)}")
    logger.info(f"🗂 Target table: {table_name}")

    logger.info(f"🧹 Clearing target table: {table_name}")
    logger.info("✅ Target table cleared")

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        conn.execute(text(insert_sql), data)

    logger.info("✅ Insert completed\n")
    
