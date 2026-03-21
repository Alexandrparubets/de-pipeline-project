from pipeline.extract import extract
from pipeline.transform import transform
from pipeline.load import (
    get_engine,
    create_orders_table,
    load_orders_to_warehouse,
    replace_incremental_tail,
    create_sales_daily_table,
    build_sales_daily,
    get_last_loaded_date
)
from pipeline.logger_config import get_logger
from pipeline.config import ensure_directories


logger = get_logger(__name__)

print("FEATURE BRANCH")

def run_pipeline():
    logger.info("Pipeline started.")

    ensure_directories()
    engine = get_engine()
    if engine is None:
        return
    try:
        create_orders_table(engine)

        last_date = get_last_loaded_date(engine)
        logger.info("Last loaded date: %s", last_date)

        raw_file = extract()
        df = transform(raw_file, last_loaded_date=last_date)

        if df.empty:
            logger.info("No new rows to load.")
            return

        #load_orders_to_warehouse(df, engine)
        replace_incremental_tail(df, engine, last_date)
      

        create_sales_daily_table(engine)
        build_sales_daily(engine)

        logger.info("Pipeline finished successfully.")

    except Exception:
        logger.error("ОШИБКА ВЫПОЛНЕНИЯ PIPELINE")
        return

if __name__ == "__main__":
    run_pipeline()