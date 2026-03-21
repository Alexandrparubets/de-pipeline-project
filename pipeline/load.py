from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pipeline.logger_config import get_logger
from pipeline.config import settings

logger = get_logger(__name__)

def get_engine():
    try:
        db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
        )

        engine = create_engine(db_url)

        # 🔥 Проверка подключения
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info(
            f"Connected to DB: {settings.db_host}:{settings.db_port}/{settings.db_name}"
        )

        return engine
        
    except Exception:
        logger.error("ОШИБКА ПОДКЛЮЧЕНИЯ к БАЗЕ ДАННЫХ")
        return None
    


def create_orders_table(engine):
    """Создает warehouse table, если она еще не существует."""

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {settings.warehouse_table} (
        invoiceno TEXT,
        stockcode TEXT,
        description TEXT,
        quantity INTEGER,
        invoicedate TIMESTAMP,
        unitprice NUMERIC(10, 2),
        customerid INTEGER,
        country TEXT,
        revenue NUMERIC(12, 2)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    #print("\nWarehouse ready: table 'orders_clean' is created (or already exists).")
    logger.info(f"Warehouse ready: table '{settings.warehouse_table}' is created (or already exists).")




def get_last_loaded_date(engine):
    query = f"""
    SELECT MAX(invoicedate) FROM {settings.warehouse_table};
    """

    with engine.begin() as conn:
        result = conn.execute(text(query)).scalar()

    return result


def load_orders_to_warehouse(df, engine):
    """Загружает DataFrame в таблицу orders_clean."""

    df_to_load = df.copy()
    # Переводим названия колонок в нижний регистр
    df_to_load.columns = [col.lower() for col in df_to_load.columns]

    df_to_load.to_sql(
        settings.warehouse_table,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=settings.chunk_size,
    )

    #print(f"Loaded {len(df_to_load)} rows into orders_clean.")
    logger.info(f"Loaded {len(df_to_load)} rows into {settings.warehouse_table}.")


    return len(df_to_load)


def replace_incremental_tail(df, engine, last_loaded_date):
    df_to_load = df.copy()
    df_to_load.columns = [col.lower() for col in df_to_load.columns]

    with engine.begin() as conn:
        if last_loaded_date is not None:
            delete_sql = text(f"""
                DELETE FROM {settings.warehouse_table}
                WHERE invoicedate >= :last_loaded_date
            """)
            result = conn.execute(delete_sql, {"last_loaded_date": last_loaded_date})
            deleted_rows = result.rowcount
        else:
             deleted_rows = 0

        df_to_load.to_sql(
            settings.warehouse_table,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=settings.chunk_size,
        )

        logger.info(
        f"Rebuilding tail from {last_loaded_date}: "
        f"deleted {deleted_rows}, inserted {len(df_to_load)}"
        )
    return len(df_to_load)


def create_sales_daily_table(engine):
    """Создает таблицу mart, если она еще не существует."""

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {settings.mart_table} (
        sales_date DATE,
        orders_cnt INTEGER,
        items_sold INTEGER,
        revenue NUMERIC(14, 2)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))

    #print("Table sales_daily is ready.")
    logger.info(f"Table {settings.mart_table} is ready.")


def build_sales_daily(engine):
    """Пересобирает витрину mart из warehouse table."""

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {settings.mart_table};"))

        conn.execute(text(f"""
            INSERT INTO {settings.mart_table}
            SELECT
                DATE(invoicedate) AS sales_date,
                COUNT(DISTINCT invoiceno) AS orders_cnt,
                SUM(quantity) AS items_sold,
                ROUND(SUM(revenue), 2) AS revenue
            FROM {settings.warehouse_table}
            GROUP BY DATE(invoicedate)
            ORDER BY sales_date;
        """))

    #print("sales_daily mart built.")
    logger.info(f"{settings.mart_table} mart built.")