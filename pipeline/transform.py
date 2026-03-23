import pandas as pd
from pipeline.logger_config import get_logger

logger = get_logger(__name__)


def transform(raw_file, last_loaded_date=None):
    """Читает raw Excel, очищает данные и возвращает DataFrame."""

    df = pd.read_excel(raw_file)

    #print(f"Initial rows: {len(df)}")
    logger.info(f"Initial rows: {len(df)}")


    # фильтр incremental
    if last_loaded_date is not None:
        df = df[df["InvoiceDate"] >= last_loaded_date]
        #print("After incremental filter:", len(df))
        logger.info(f"After incremental filter: {len(df)}")


    # удаляем отмененные транзакции df = df[~условие] - оставь строки, где условие НЕ выполняется (~ это инверсия)
    df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]

    # удаляем строки с неположительным количеством
    df = df[df["Quantity"] > 0]

    # удаляем строки без CustomerID dropna - удалить subset= - на какую колонку ориентируемся
    df = df.dropna(subset=["CustomerID"])

    # делаем копию после фильтрации. df.copy() = создать независимую копию
    df = df.copy()

    # приводим CustomerID к int
    df["CustomerID"] = df["CustomerID"].astype(int)

    # считаем выручку
    df["Revenue"] = (df["Quantity"] * df["UnitPrice"]).round(2)



    # удаляем строки с нулевой или отрицательной выручкой
    df = df[df["Revenue"] > 0]

    # проверяем дубли по 2 колонкам
    duplicates = df.duplicated(subset=["InvoiceNo", "StockCode"]).sum()
    logger.info(f"Duplicates by InvoiceNo+StockCode: {duplicates}")

    # проверяем полные дубли
    full_duplicates = df.duplicated().sum()
    logger.info(f"Full row duplicates: {full_duplicates}")

    # удаляем дубли
    df = df.drop_duplicates()

    #print(f"Clean rows: {len(df)}")
    logger.info(f"Clean rows: {len(df)}")

    return df

# TODO: dedup logic experiment