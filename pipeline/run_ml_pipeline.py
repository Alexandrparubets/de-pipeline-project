from pipeline.connection import get_engine
from pipeline.setup_db import create_ml_table, create_cf_table
from pipeline.load_ml_table import load_data_ml
from pipeline.load_customer_features import load_cf_table
from pipeline.load_ml_to_df import load_ml_dataset
from pipeline.logger_config import get_logger
from pipeline.config import settings

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report


logger = get_logger("pipeline.run_ml")


def run_ml_pipeline():
    logger.info("🚀 ML pipeline started")

    engine = get_engine()

    create_ml_table(engine)
    load_data_ml(engine) # load_ml_table.py
    create_cf_table(engine)
    load_cf_table(engine) #load_customer_features.py

    X, y = load_ml_dataset(engine)
    
    X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    )

    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    # y_pred = model.predict(X_test)
    

    y_prob = model.predict_proba(X_test)[:, 1]

    y_pred_new = (y_prob > 0.4).astype(int)

    

    
    print(classification_report(y_test, y_pred_new))

    logger.info(f"✅ ML pipeline finished\n --------------------------------------------- python -m pipeline.run_ml_pipeline")

    return X, y


if __name__ == "__main__":
    run_ml_pipeline()