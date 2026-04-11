from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.config import settings
from pipeline.logger_config import get_logger


logger = get_logger("pipeline.setup_db")


def setup_database(engine: Engine) -> None:
    """
    Prepares database objects for pipeline execution.
    Creates tables and constraints if they do not exist.
    Does not delete or overwrite existing data.
    """
    drop_stg_table(engine)
    create_stg_table(engine)
    drop_raw_stg_table(engine)
    create_raw_stg_table(engine)
    create_dwh_table(engine)
    create_mart_table(engine)
    create_pipeline_runs_table(engine)


def create_stg_table(engine: Engine) -> None:
    table_name = settings.stg_table
    stg_schema = settings.stg_schema

    if not stg_schema:
        raise ValueError("STG schema is empty")

    columns_sql = ",\n        ".join(
        f"{column} {data_type}" for column, data_type in stg_schema.items()
    )

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql},
        CONSTRAINT uq_row_hash UNIQUE (row_hash)
    );
    """

    logger.debug(f"STG CREATE SQL:\n{create_sql}")

    with engine.begin() as conn:
        conn.execute(text(create_sql))

    logger.info(
        f"🟢 Staging ready: table '{table_name}' is created (or already exists)."
    )


def create_raw_stg_table(engine: Engine) -> None:
    table_name = settings.raw_stg_table
    raw_stg_schema = settings.raw_stg_schema

    if not raw_stg_schema:
        raise ValueError("RAW STG schema is empty")

    columns_sql = ",\n        ".join(
        f"{column} {data_type}" for column, data_type in raw_stg_schema.items()
    )

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    """

    logger.debug(f"RAW STG CREATE SQL:\n{create_sql}")

    with engine.begin() as conn:
        conn.execute(text(create_sql))

    logger.info(
        f"🟢 Staging ready: table '{table_name}' is created (or already exists)."
    )



def truncate_dwh_table(engine: Engine) -> None:
    table_name = settings.dwh_table

    truncate_sql = f"""
    TRUNCATE TABLE {table_name}
    RESTART IDENTITY CASCADE;
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(truncate_sql))

        logger.warning(
            f"🧹 DWH table '{table_name}' truncated due to historical data change."
        )

    except Exception as e:
        logger.exception(f"Error truncating table '{table_name}': {e}")
        raise



def create_dwh_table(engine: Engine) -> None:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {settings.dwh_table} (
        id BIGSERIAL PRIMARY KEY,
        row_hash TEXT NOT NULL,
        invoiceno TEXT NOT NULL,
        stockcode TEXT NOT NULL,
        description TEXT,
        quantity INTEGER NOT NULL,
        invoicedate TIMESTAMP NOT NULL,
        unitprice NUMERIC(12, 4) NOT NULL,
        customerid INTEGER NOT NULL,
        country TEXT,
        revenue NUMERIC(14, 2) NOT NULL
    );
    """

    add_unique_constraint_sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'uq_{settings.dwh_table}_row_hash'
        ) THEN
            ALTER TABLE {settings.dwh_table}
            ADD CONSTRAINT uq_{settings.dwh_table}_row_hash UNIQUE (row_hash);
        END IF;
    END
    $$;
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        conn.execute(text(add_unique_constraint_sql))

    logger.info(
        f"📦 Warehouse ready: table '{settings.dwh_table}' is created (or already exists)."
    )
    logger.info(
        f"🔐 Constraint applied: UNIQUE(row_hash) on '{settings.dwh_table}'."
    )


def create_mart_table(engine: Engine) -> None:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {settings.mart_table} (
        order_date DATE PRIMARY KEY,
        total_orders INTEGER NOT NULL,
        total_quantity INTEGER NOT NULL,
        total_revenue NUMERIC(14, 2) NOT NULL
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    logger.info(
        f"📊 Mart ready: table '{settings.mart_table}' is created (or already exists)."
    )


def create_pipeline_runs_table(engine: Engine) -> None:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {settings.pipeline_runs_table} (
        id SERIAL PRIMARY KEY,
        pipeline_name TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP,
        watermark_value TIMESTAMP,
        boundary_date TIMESTAMP,
        historical_hash TEXT,
        rows_in_stg INTEGER,
        rows_loaded_to_dwh INTEGER,
        rows_skipped_in_dwh INTEGER,
        error_message TEXT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    logger.info(
        f"🧾 Metadata ready: table '{settings.pipeline_runs_table}' is created (or already exists).\n"
    )


def drop_stg_table(engine: Engine) -> None:
    table_name = settings.stg_table

    drop_sql = f"DROP TABLE IF EXISTS {table_name};"

    with engine.begin() as conn:
        conn.execute(text(drop_sql))

    logger.info(f"🧹Staging dropped: table '{table_name}' was removed.")


def drop_raw_stg_table(engine: Engine) -> None:
    table_name = settings.raw_stg_table

    drop_sql = f"DROP TABLE IF EXISTS {table_name};"

    with engine.begin() as conn:
        conn.execute(text(drop_sql))

    logger.info(f"🧹 Staging dropped: table '{table_name}' was removed.")