import logging
from pathlib import Path
from contextvars import ContextVar

from pipeline.config import settings


current_run_id: ContextVar[str] = ContextVar("run_id", default="SYSTEM")


def set_run_id(run_id: str) -> None:
    current_run_id.set(str(run_id))


def get_run_id() -> str:
    return current_run_id.get()


class RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = get_run_id()
        return True


def get_logger(name: str) -> logging.Logger:
    log_path = Path(settings.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(settings.log_level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | run_id=%(run_id)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    run_id_filter = RunIdFilter()
    file_handler.addFilter(run_id_filter)
    console_handler.addFilter(run_id_filter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger