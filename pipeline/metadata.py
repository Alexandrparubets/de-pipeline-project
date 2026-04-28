from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.config import settings
from pipeline.logger_config import get_logger


logger = get_logger("pipeline.metadata")


def start_pipeline_run(engine: Engine, pipeline_name: str) -> int:
    """
    Creates a new record in pipeline_runs table
    and returns generated run_id.
    """
    insert_sql = f"""
    INSERT INTO {settings.pipeline_runs_table} (
        pipeline_name,
        status,
        started_at
    )
    VALUES (
        :pipeline_name,
        'running',
        :started_at
    )
    RETURNING id;
    """

    with engine.begin() as conn:
        run_id = conn.execute(
            text(insert_sql),
            {
                "pipeline_name": pipeline_name,
                "started_at": datetime.now(),
            },
        ).scalar()

    logger.info(
        f"🚀 Pipeline run started: Pipeline: {pipeline_name}"
    )
    logger.info(f"🆔 New id = {run_id}")

    return run_id


def finish_pipeline_run_success(
    engine: Engine,
    run_id: int,
    watermark_value=None,
    boundary_date = None,
    historical_hash: str | None =  None,
    rows_in_stg: int | None = None,
    rows_loaded_to_dwh: int | None = None,
    rows_skipped_in_dwh: int | None = None,
) -> None:
    """
    Updates pipeline run record with success status
    and final execution statistics.
    """
    update_sql = f"""
    UPDATE {settings.pipeline_runs_table}
    SET
        status = 'success',
        finished_at = :finished_at,
        watermark_value = :watermark_value,
        boundary_date = :boundary_date,
        historical_hash = :historical_hash,
        rows_in_stg = :rows_in_stg,
        rows_loaded_to_dwh = :rows_loaded_to_dwh,
        rows_skipped_in_dwh = :rows_skipped_in_dwh,
        error_message = NULL
    WHERE id = :run_id;
    """

    with engine.begin() as conn:
        conn.execute(
            text(update_sql),
            {
                "run_id": run_id,
                "finished_at": datetime.now(),
                "watermark_value": watermark_value,
                "boundary_date": boundary_date,
                "historical_hash": historical_hash,
                "rows_in_stg": rows_in_stg,
                "rows_loaded_to_dwh": rows_loaded_to_dwh,
                "rows_skipped_in_dwh": rows_skipped_in_dwh,
            },
        )

    logger.info(
        f"✅ Pipeline run finished successfully: id={run_id}, status='success'\n"
    )


def finish_pipeline_run_failed(
    engine: Engine,
    run_id: int,
    error_message: str,
) -> None:
    """
    Updates pipeline run record with failed status
    and error message.
    """
    update_sql = f"""
    UPDATE {settings.pipeline_runs_table}
    SET
        status = 'failed',
        finished_at = :finished_at,
        error_message = :error_message
    WHERE id = :run_id;
    """

    with engine.begin() as conn:
        conn.execute(
            text(update_sql),
            {
                "run_id": run_id,
                "finished_at": datetime.now(),
                "error_message": error_message,
            },
        )

    logger.error(
        f"Pipeline run failed: id={run_id}, status='failed', error='{error_message}'"
    )


def get_last_successful_watermark(engine: Engine, pipeline_name: str):
    """
    Returns watermark_value and boundary_date from the last successful pipeline run.
    If no successful run exists, returns None.
    """
    select_sql = f"""
    SELECT watermark_value, boundary_date
    FROM {settings.pipeline_runs_table}
    WHERE pipeline_name = :pipeline_name
      AND status = 'success'
    ORDER BY finished_at DESC
    LIMIT 1;
    """

    with engine.begin() as conn:
        result = conn.execute(
            text(select_sql),
            {"pipeline_name": pipeline_name},
        ).fetchone()

    last_watermark = result.watermark_value
    boundary_date = result.boundary_date 

    if result is not None:
        last_watermark = result.watermark_value
        boundary_date = result.boundary_date
        logger.info(f"📅 Last watermark: {last_watermark}")
        logger.info(f"🧱 Last boundary date: {boundary_date}")
    else:
        logger.info(f"⚠️ No successful watermark found.")
        logger.info(f"⚠️ No successful boundary date found.")

    if result is None:
        return None, None

     

    return last_watermark, boundary_date


def get_last_successful_historical_hash(engine, pipeline_name: str) -> str | None:
    query = """
    SELECT historical_hash
    FROM pipeline_runs
    WHERE pipeline_name = :pipeline_name
      AND status = 'success'
    ORDER BY finished_at DESC
    LIMIT 1;
    """

    with engine.begin() as conn:
        result = conn.execute(
            text(query),
            {"pipeline_name": pipeline_name},
        ).scalar()

    if result is not None:
        logger.info(f"🧬 Last historical hash: {result}\n")
    else:
        logger.info(f"⚠️ No successful watermark found.")
        

    return result