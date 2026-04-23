from pipeline.connection import get_engine, test_connection
from pipeline.setup_db import create_ml_models_table, create_ml_model_baselines_table
from pipeline.build_ml_dataset import build_ml_dataset_df
from pipeline.logger_config import get_logger
from pipeline.config import settings
from pipeline.train_model import train_model, save_model
from pipeline.load_ml_models import load_ml_models_table
from pipeline.build_drift_baseline import build_drift_baseline
from pipeline.load_drift_baseline import load_drift_baseline_table



logger = get_logger("pipeline.run_train")


def run_train():
    logger.info("🚀 ML train started\n")

    engine = get_engine()
    test_connection(engine)
    create_ml_models_table(engine)
    create_ml_model_baselines_table(engine)

    f_start = settings.f_start
    f_end = settings.f_end
    t_start = settings.t_start
    t_end = settings.t_end
    dwh_table = settings.dwh_table
    threshold = settings.threshold
    model_path = settings.model_path

    logger.info(
    f"🪟 Train windows: f_start={f_start}, "
    f"f_end={f_end}, t_start={t_start}, t_end={t_end}"
    )
    
    df, X, y = build_ml_dataset_df(engine, dwh_table, f_start, f_end, t_start, t_end) #build_ml_dataset.py
    
    logger.info("📥 Training dataset loaded successfully")
    logger.info(f"📊 Train dataset shape: df={df.shape}, X={X.shape}, y={y.shape}\n")

    model, roc_auc = train_model(X, y) # train_model.py

    model_name = type(model).__name__

    save_model(model) # train_model.py

    model_id = load_ml_models_table(
    engine,
    model_name,
    model_path,
    roc_auc,
    threshold
    ) # load_ml_models

    baseline_df = build_drift_baseline(X, model_id)

    load_drift_baseline_table(engine, baseline_df)

    logger.info(f"✅ ML train finished\n --------------------------------------------- python -m pipeline.run_train")

    return X, y


if __name__ == "__main__":
    run_train()