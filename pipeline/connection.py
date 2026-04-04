from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pipeline.config import settings
from pipeline.logger_config import get_logger


logger = get_logger("pipeline.connection")


def get_engine() -> Engine:
    """
    Creates and returns SQLAlchemy engine
    """

    db_url = (
        f"postgresql+psycopg2://{settings.db_user}:"
        f"{settings.db_password}@{settings.db_host}:"
        f"{settings.db_port}/{settings.db_name}"
    )

    engine = create_engine(db_url)

    logger.info(
        f"Connected to DB: {settings.db_host}:{settings.db_port}/{settings.db_name}"
    )

    return engine


def test_connection(engine: Engine) -> None:
    """
    Simple connection check
    """

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection successful")
    except Exception as e:
        logger.exception(f"❌ Database connection failed: {e}")
        raise