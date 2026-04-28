from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def get_active_model(engine, ml_models_table) -> tuple[str, float]:
    logger.info("📥 Loading active model metadata")

    query = text(f"""
        SELECT id, model_path, threshold
        FROM {ml_models_table}
        WHERE is_active = TRUE
        ORDER BY model_version DESC
        LIMIT 1
    """)

    with engine.begin() as conn:
        result = conn.execute(query).fetchone()

    if result is None:
        raise ValueError("No active model found in ml_models table")

    model_id, model_path, threshold = result

    logger.info(f"📦 Active model loaded: path={model_path}, threshold={threshold}, model_id={model_id}\n")

    return model_id, model_path, threshold