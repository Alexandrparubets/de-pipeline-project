import joblib
import pandas as pd
import numpy as np
from sqlalchemy import text


def save_model(model):

    joblib.dump(model, "model.pkl")


def score_model(model, X):

    model = joblib.load("model.pkl")

    y_prob = model.predict_proba(X)[:, 1]

    return y_prob


def model_to_db(df, X, y_prob):

    import numpy as np

    if len(X) != len(y_prob):
        raise ValueError("Length mismatch between X and y_prob")

    df_result = df.loc[X.index].copy()

    df_result["probability"] = np.array(y_prob)

    df_result = df_result[["customerid", "probability"]]

    return df_result


def insert_scores(engine, df_result, table_name: str) -> None:
    
    if df_result.empty:
        return

    insert_sql = f"""
        INSERT INTO {table_name} (customerid, probability)
        VALUES (:customerid, :probability)
    """

    data = df_result.to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text(insert_sql), data)
    
