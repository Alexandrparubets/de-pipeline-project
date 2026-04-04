from pipeline.logger_config import get_logger, set_run_id
from pipeline.connection import get_engine, test_connection


logger = get_logger("pipeline.run")


def run_pipeline() -> None:
   
    logger.info("🚀 Pipeline process started")

    engine = get_engine()
    test_connection(engine)

    logger.info("✅ Pipeline finished")


if __name__ == "__main__":
    run_pipeline()