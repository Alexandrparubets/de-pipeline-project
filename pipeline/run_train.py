from pipeline.connection import get_engine, test_connection
from pipeline.build_ml_dataset import build_ml_dataset_df
from pipeline.logger_config import get_logger
from pipeline.config import settings
from pipeline.train_model import train_model, save_model
from pipeline.score_model import score_model, model_to_db, insert_scores




logger = get_logger("pipeline.run_train")


def run_train():
    logger.info("🚀 ML train started\n")

    engine = get_engine()
    test_connection(engine)

    f_start = settings.f_start
    f_end = settings.f_end
    t_start = settings.t_start
    t_end = settings.t_end
    dwh_table = settings.dwh_table

    logger.info(
    f"🪟 Train windows: f_start={f_start}, "
    f"f_end={f_end}, t_start={t_start}, t_end={t_end}"
    )
    
    df, X, y = build_ml_dataset_df(engine, dwh_table, f_start, f_end, t_start, t_end) #build_ml_dataset.py
    
    logger.info("📥 Training dataset loaded successfully")
    logger.info(f"📊 Train dataset shape: df={df.shape}, X={X.shape}, y={y.shape}\n")

    model = train_model(X, y) # train_model.py

    save_model(model) # train_model.py

    logger.info(f"✅ ML train finished\n --------------------------------------------- python -m pipeline.run_train")

    return X, y


if __name__ == "__main__":
    run_train()