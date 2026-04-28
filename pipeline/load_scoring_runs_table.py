from sqlalchemy import text

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_scoring_runs_table(
    engine,
    run_id: int,
    model_id: int,
    rows_count: int,
    f_start: int,
    f_end: int,
    drift_detected_mean: bool,
    drift_detected_std: bool,
    drift_threshold: float,
) -> int:
    logger.info("📥 Starting scoring_runs table load")

    insert_sql = text(f"""
        INSERT INTO {settings.scoring_runs_table} (
            id,
            model_id,
            rows_count,
            f_start,
            f_end,
            drift_detected_mean,
            drift_detected_std,
            drift_threshold
        )
        VALUES (
            :id,
            :model_id,
            :rows_count,
            :f_start,
            :f_end,
            :drift_detected_mean,
            :drift_detected_std,
            :drift_threshold
        )
        RETURNING id
    """)

    params = {
        "id": int(run_id),
        "model_id": int(model_id),
        "rows_count": int(rows_count),
        "f_start": int(f_start),
        "f_end": int(f_end),
        "drift_detected_mean": bool(drift_detected_mean),
        "drift_detected_std": bool(drift_detected_std),
        "drift_threshold": float(drift_threshold),
    }

    with engine.begin() as conn:
        scoring_run_id = conn.execute(insert_sql, params).scalar()

    logger.info(f"🆔 scoring_run_id: {scoring_run_id}")
    logger.info("✅ scoring_runs table load completed")

    return scoring_run_id