import pandas as pd
from pipeline.config import settings
from sqlalchemy import text

from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def build_current_stats(X: pd.DataFrame) -> pd.DataFrame:
    logger.info("📊 Starting current stats build")

    current_rows = []

    for col in X.columns:
        current_rows.append({
            "feature_name": col,
            "mean_value": X[col].mean(),
            "std_value": X[col].std(),
            "median_value": X[col].median(),
            "q25_value": X[col].quantile(0.25),
            "q75_value": X[col].quantile(0.75),
        })

    current_df = pd.DataFrame(current_rows)

    logger.info(f"📦 Current stats built: rows={len(current_df)}")
    logger.info(f"🧩 Current features: {list(X.columns)}")

    return current_df


def load_baseline(engine, model_id: int) -> pd.DataFrame:
    query = text(f"""
        SELECT feature_name,
               mean_value,
               std_value,
               median_value,
               q25_value,
               q75_value
        FROM {settings.ml_model_baselines_table}
        WHERE model_id = :model_id
    """)

    return pd.read_sql(query, engine, params={"model_id": model_id})


def drift_check(baseline_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔍 Starting drift check")

    df_compare = baseline_df.merge(
        current_df,
        on="feature_name",
        suffixes=("_base", "_curr")
    )

    df_compare["mean_diff_pct"] = (
        (df_compare["mean_value_curr"] - df_compare["mean_value_base"])
        / (df_compare["mean_value_base"] + 1e-9)
        * 100
    )
    df_compare["std_diff_pct"] = (
    (df_compare["std_value_curr"] - df_compare["std_value_base"])
    / (df_compare["std_value_base"] + 1e-9)
    * 100
    )

    df_compare["median_diff_pct"] = (
        (df_compare["median_value_curr"] - df_compare["median_value_base"])
        / (df_compare["median_value_base"] + 1e-9)
        * 100
    )

    df_compare["q25_diff_pct"] = (
        (df_compare["q25_value_curr"] - df_compare["q25_value_base"])
        / (df_compare["q25_value_base"] + 1e-9)
        * 100
    )

    df_compare["q75_diff_pct"] = (
        (df_compare["q75_value_curr"] - df_compare["q75_value_base"])
        / (df_compare["q75_value_base"] + 1e-9)
        * 100
    )

    threshold = 20

    df_compare["is_drift"] = (
        (df_compare["mean_diff_pct"].abs() > threshold) |
        (df_compare["std_diff_pct"].abs() > threshold) |
        (df_compare["median_diff_pct"].abs() > threshold) |
        (df_compare["q25_diff_pct"].abs() > threshold) |
        (df_compare["q75_diff_pct"].abs() > threshold)
    )

    cols = [
        "mean_diff_pct",
        "std_diff_pct",
        "median_diff_pct",
        "q25_diff_pct",
        "q75_diff_pct",
    ]

    df_compare[cols] = df_compare[cols].round(0).astype(int)

    df_log = df_compare[[
        "feature_name",
        "mean_diff_pct",
        "std_diff_pct",
        "median_diff_pct",
        "q25_diff_pct",
        "q75_diff_pct",
        "is_drift",
    ]].copy()

    cols = [
    "mean_diff_pct",
    "std_diff_pct",
    "median_diff_pct",
    "q25_diff_pct",
    "q75_diff_pct",
    ]

    def highlight(val, threshold):
        val = int(val)
        return f"🔴 {val}%" if abs(val) > threshold else f"{val}%"

    for col in cols:
        df_log[col] = df_log[col].apply(lambda x: highlight(x, threshold))

    df_log["is_drift"] = df_log["is_drift"].map({
        True: "🔴 drift",
        False: "ok"
    })

    logger.info(f"⚙️ DRIFT_THRESHOLD: {threshold}%")

    with pd.option_context(
        "display.max_columns", None,
        "display.width", 2000,
        "display.max_colwidth", None
    ):
        logger.info(f"📊 Drift summary:\n{df_log}\n")

    return df_compare


