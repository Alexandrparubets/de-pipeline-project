from pipeline.connection import get_engine, test_connection
from pipeline.build_ml_dataset import build_ml_dataset_df
from pipeline.setup_db import create_c_score_table
from pipeline.logger_config import get_logger
from pipeline.config import settings
from pipeline.score_model import score_model, model_to_db, insert_scores




logger = get_logger("pipeline.run_scoring")


def run_scoring():
    logger.info("🚀 ML scoring started\n")

    engine = get_engine()
    test_connection(engine)

    shift = 20

    f_start = settings.f_start + shift
    f_end = settings.f_end + shift
    t_start = settings.t_start + shift
    t_end = settings.t_end + shift
    dwh_table = settings.dwh_table

    logger.info(
    f"🪟 Scoring windows: f_start={f_start}, "
    f"f_end={f_end}, t_start={t_start}, t_end={t_end}\n"
    )
    
    df, X, y = build_ml_dataset_df(engine, dwh_table, f_start, f_end, t_start, t_end) #build_ml_dataset.py
    
    logger.info("📥 Scoring dataset loaded successfully")
    logger.info(f"📊 Scoring dataset shape: df={df.shape}, X={X.shape}\n")

    y_prob = score_model(X) # score_model.py

    df_result = model_to_db(df, X, y_prob) # score_model.py

    create_c_score_table(engine) # setup_db.py

    insert_scores(engine, df_result, settings.c_scores) # score_model.py

    logger.info(f"✅ ML Scoring finished\n --------------------------------------------- python -m pipeline.run_scoring")

    return X, y


if __name__ == "__main__":
    run_scoring()