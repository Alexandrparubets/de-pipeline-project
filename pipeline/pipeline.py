from pipeline.logger_config import get_logger, set_run_id
from pipeline.connection import get_engine, test_connection
from pipeline.setup_db import setup_database
from pipeline.metadata import (start_pipeline_run,
    finish_pipeline_run_success,
    finish_pipeline_run_failed,
    get_last_successful_watermark,
    get_last_successful_historical_hash
    )
from pipeline.extract import get_source_file_path
from pipeline.raw import create_raw_copy
from pipeline.transform import load_raw_to_dataframe#, clean_dataframe, calculate_historical_hash
from pipeline.load_raw_stg import load_to_raw_stg, align_to_raw_stg_columns
from pipeline.load_stg import load_raw_stg_to_stg, get_last_watermark_value
from pipeline.quality import run_quality_checks
from pipeline.load_dwh import load_stg_to_dwh
from pipeline.load_mart import load_data_mart
from pipeline.load_ml_table import load_data_ml
from pipeline.historical_hash import get_new_boundary_date, get_historical_hash, check_historical_hash
from pipeline.load_ml_to_df import load_ml_dataset


logger = get_logger("pipeline.run")
pipeline_name = "NEW PIPELINE"

def run_pipeline() -> None:
   
    logger.info("🚀🚀🚀 Pipeline process started\n")

    try:

        
        engine = get_engine() # connection.py
        test_connection(engine) # connection.py
        setup_database(engine) # setup_db.py
        run_id = start_pipeline_run(engine, pipeline_name) # metadata.py
        set_run_id(run_id) # logger.config
        last_watermark, boundary_date = get_last_successful_watermark(engine, pipeline_name) # metadata.py
        last_historical_hash = get_last_successful_historical_hash(engine, pipeline_name) # metadata.py
        source_file = get_source_file_path() # extract.py
        raw_file_path, file_hash = create_raw_copy(source_file, pipeline_name) # raw.py
        df = load_raw_to_dataframe(engine, pipeline_name, raw_file_path,  boundary_date) # transform.py
        df = align_to_raw_stg_columns(df) # load_raw_stg
        rows_in_raw_stg = load_to_raw_stg(df, engine) # load_raw_stg
        last_watermark = check_historical_hash(engine, last_historical_hash, boundary_date, last_watermark) # historical_hash.py
        new_boundary_date = get_new_boundary_date(engine) # historical_hash.py
        new_historical_hash = get_historical_hash(engine, new_boundary_date, 1) # historical_hash.py
        rows_in_stg = load_raw_stg_to_stg(engine, last_watermark) # load_stg.py

        if rows_in_stg ==0:
            finish_pipeline_run_success(
                engine=engine,
                run_id=run_id,
                rows_in_stg=0,
                watermark_value=last_watermark,
                boundary_date = new_boundary_date,
                historical_hash=new_historical_hash,
                rows_loaded_to_dwh=0,
                rows_skipped_in_dwh=0,
            )
            logger.info("⚠️ No rows to process after STG load. Pipeline finished early.")
            return


        watermark_value = get_last_watermark_value(engine) # load_stg.py
       

        run_quality_checks(engine) # quality.py
        dwh_stats = load_stg_to_dwh(engine) # load_dwh.py
        attempted_rows = dwh_stats["attempted_rows"]
        inserted_rows = dwh_stats["inserted_rows"]
        skipped_rows = dwh_stats["skipped_rows"]
        mart_rows = load_data_mart(engine) # load_mart.py
        ml_rows = load_data_ml(engine) # load_ml_table.py
        X, y = load_ml_dataset(engine) # load_ml_to_df.py
        

        finish_pipeline_run_success(
            engine=engine,
            run_id=run_id,
            rows_in_stg=rows_in_raw_stg,
            watermark_value=watermark_value,
            boundary_date = new_boundary_date,
            historical_hash=new_historical_hash,
            rows_loaded_to_dwh=inserted_rows,
            rows_skipped_in_dwh=skipped_rows,
        )
        logger.info("✅ Pipeline finished\n --------------------------------------------- python -m pipeline.pipeline")

    except ValueError as e:
        finish_pipeline_run_failed(
            engine=engine,
            run_id=run_id,
            error_message=str(e),
        )
        logger.error(f"Pipeline stopped due to validation error: {e}")
        return    

    except Exception as e:
        finish_pipeline_run_failed(
            engine=engine,
            run_id=run_id,
            error_message=str(e),
        )
        raise

if __name__ == "__main__":
    run_pipeline()