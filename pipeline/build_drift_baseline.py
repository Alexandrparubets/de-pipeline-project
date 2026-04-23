import pandas as pd

from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def build_drift_baseline(X: pd.DataFrame, model_id: int) -> pd.DataFrame:
    logger.info("📊 Starting drift baseline build")

    baseline_rows = []

    for col in X.columns:
        baseline_rows.append({
            "model_id": model_id,
            "feature_name": col,
            "mean_value": X[col].mean(),
            "std_value": X[col].std(),
            "median_value": X[col].median(),
            "q25_value": X[col].quantile(0.25),
            "q75_value": X[col].quantile(0.75),
        })

    baseline_df = pd.DataFrame(baseline_rows)

    logger.info(f"📦 Drift baseline built: rows={len(baseline_df)}")
    logger.info(f"🧩 Baseline features: {list(X.columns)}\n")

    return baseline_df