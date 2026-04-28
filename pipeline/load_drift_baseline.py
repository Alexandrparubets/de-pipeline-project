import pandas as pd

from pipeline.config import settings
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def load_drift_baseline_table(engine, baseline_df: pd.DataFrame) -> None:
    logger.info("📥 Starting drift baseline load")

    if baseline_df.empty:
        logger.warning("⚠️ Baseline DataFrame is empty")
        return

    baseline_df.to_sql(
        settings.ml_model_baselines_table,
        engine,
        if_exists="append",
        index=False,
    )

    logger.info(f"📦 Baseline rows inserted: {len(baseline_df)}")
    logger.info("✅ Drift baseline load completed\n")