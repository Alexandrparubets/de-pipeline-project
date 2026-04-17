import pandas as pd
from sklearn.model_selection import train_test_split
#from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
from pipeline.logger_config import get_logger

logger = get_logger(__name__)



def train_model(X, y):
    logger.info("🚀 Starting model training")

    X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    )
    logger.info(f"📊 Train shape: {X_train.shape}, Test shape: {X_test.shape}")
    # model = RandomForestClassifier(
    # n_estimators=100,
    # random_state=42,
    # )
    model = GradientBoostingClassifier(
    n_estimators=200,
    learning_rate=0.05,
    max_depth=3,
    random_state=42
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    
    logger.info("\n" + classification_report(y_test, y_pred))
    logger.info(f"ROC AUC: {roc_auc_score(y_test, y_prob):.4f}")

    
    importances = model.feature_importances_
    feature_importance_df = pd.DataFrame({
        "feature": X.columns,
        "importance": importances
    }).sort_values(by="importance", ascending=False)

    logger.info("\n" + feature_importance_df.to_string())

    logger.info("✅ Model training completed \n")

    return model