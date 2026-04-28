from pipeline.connection import get_engine
from pipeline.setup_db import create_ml_table, create_cf_table, create_c_score_table
from pipeline.load_ml_table import load_data_ml
from pipeline.load_customer_features import load_cf_table
from pipeline.load_ml_to_df import load_ml_dataset
from pipeline.logger_config import get_logger
from pipeline.config import settings
from pipeline.train_model import train_model, save_model
from pipeline.score_model import score_model, model_to_db, insert_scores




logger = get_logger("pipeline.run_ml")


def run_ml_pipeline():
    logger.info("🚀 ML pipeline started")

    engine = get_engine()
    f_start = settings.f_start
    f_end = settings.f_end

    create_ml_table(engine) # setup_db.py
    create_cf_table(engine) # setup_db.py
    load_cf_table(engine, f_start, f_end) #load_customer_features.py
    load_data_ml(engine) # load_ml_table.py

    X, y, df = load_ml_dataset(engine) # load_ml_to_df.py
    
    model = train_model(X, y) # train_model.py

    save_model(model) # train_model.py

    y_prob = score_model(model, X) # score_model.py

    df_result = model_to_db(df, X, y_prob) # score_model.py

    create_c_score_table(engine) # setup_db.py

    insert_scores(engine, df_result, settings.c_scores) # score_model.py

    logger.info(f"✅ ML pipeline finished\n --------------------------------------------- python -m pipeline.run_ml_pipeline")

    return X, y


if __name__ == "__main__":
    run_ml_pipeline()