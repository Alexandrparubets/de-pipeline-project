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
    threshold: float,
) -> None:
    logger.info("📥 Starting ML models table load")

    if not (0 <= settings.roc_auc_threshold <= 1):
        raise ValueError(f"Invalid ROC_AUC_THRESHOLD: {settings.roc_auc_threshold}")

    is_active = roc_auc >= settings.roc_auc_threshold

    if not is_active:
        logger.info(
            f"⏭ Model skipped: roc_auc={roc_auc:.4f} "
            f"is below threshold={settings.roc_auc_threshold:.4f}"
        )
        return

    with engine.begin() as conn:
        model_version_sql = text(f"""
            SELECT COALESCE(MAX(model_version), 0) + 1
            FROM {settings.ml_models_table}
        """)
        model_version = conn.execute(model_version_sql).scalar()

        conn.execute(
            text(f"UPDATE {settings.ml_models_table} SET is_active = FALSE")
        )
        logger.info("🔄 Previous active models deactivated")

        df = pd.DataFrame([{
            "model_name": model_name,
            "model_path": model_path,
            "model_version": model_version,
            "roc_auc": roc_auc,
            "threshold": threshold,
            "is_active": True,
        }])

        df.to_sql(
            settings.ml_models_table,
            con=conn,
            if_exists="append",
            index=False,
        )

    logger.info(
        f"📦 Model metadata inserted: "
        f"name={model_name}, path={model_path}, model_id={model_version}, "
        f"roc_auc={roc_auc:.4f}, threshold={threshold}"
    )
    logger.info("✅ ML models table load completed\n")

    return model_version