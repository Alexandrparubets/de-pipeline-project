import pandas as pd
from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_ml_models_table(
    engine,
    model_name: str,
    model_path: str,
    roc_auc: float,
    f_start: int,
    f_end: int,
    t_start: int,
    t_end: int,
    threshold: float,
) -> None:
    logger.info("📥 Starting ML models table load")

    if not (0 <= settings.roc_auc_threshold <= 1):
        raise ValueError(f"Invalid ROC_AUC_THRESHOLD: {settings.roc_auc_threshold}")

    is_test = roc_auc >= settings.roc_auc_threshold

    if not is_test:
        logger.info(
            f"⏭ Model skipped: roc_auc={roc_auc:.4f} "
            f"is below threshold={settings.roc_auc_threshold:.4f}"
        )
        

    with engine.begin() as conn:
        model_version_sql = text(f"""
            SELECT COALESCE(MAX(model_version), 0) + 1
            FROM {settings.ml_models_table}
        """)
        model_version = conn.execute(model_version_sql).scalar()

        if is_test:
            # conn.execute(
            #     text(f"UPDATE {settings.ml_models_table} SET is_active = FALSE")
            # )
            is_active = True
            # logger.info("🔄 Previous active models deactivated")
        else:
            is_active = False
            model_path = None

    insert_sql = text(f"""
        INSERT INTO {settings.ml_models_table} (
            model_name,
            model_path,
            model_version,
            roc_auc,
            threshold,
            f_start,
            f_end,
            t_start,
            t_end,
            is_active,
            is_test
        )
        VALUES (
            :model_name,
            :model_path,
            :model_version,
            :roc_auc,
            :threshold,
            :f_start,
            :f_end,
            :t_start,
            :t_end,
            :is_active,
            :is_test
        )
        RETURNING id
    """)
    params = {
        "model_name": model_name,
        "model_path": model_path,
        "model_version": model_version,
        "roc_auc": roc_auc,
        "threshold": threshold,
        "f_start": f_start,
        "f_end": f_end,
        "t_start": t_start,
        "t_end": t_end,
        "is_test": is_test,
        "is_active": is_active,
    }

    with engine.begin() as conn:
        if is_active:
            conn.execute(
                text(f"UPDATE {settings.ml_models_table} SET is_active = FALSE")
            )
            logger.info("🔄 Previous active models deactivated")

        result = conn.execute(insert_sql, params)
        train_id = result.scalar()

    logger.info(f"🆔 model_id: {train_id}")

        

    logger.info(
        f"📦 Model metadata inserted: "
        f"name={model_name}, path={model_path}, model_id={model_version}, "
        f"🧪 Test result: is_test={is_test},"
        f"roc_auc={roc_auc:.4f}, threshold={threshold}"
    )
    logger.info("✅ ML models table load completed\n")

    return model_version, train_id