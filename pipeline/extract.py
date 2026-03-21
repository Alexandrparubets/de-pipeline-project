
import shutil
from datetime import datetime
import hashlib
from pipeline.logger_config import get_logger
from pipeline.config import settings

logger = get_logger(__name__)


def file_hash(path):
    """Вычисляет SHA256 hash файла"""
    h = hashlib.sha256()

    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)

    return h.hexdigest()


def extract():
    

    source_file = settings.source_dir / settings.source_file
    raw_dir = settings.raw_dir
    

    if not source_file.exists():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    source_hash = file_hash(source_file)

    # ищем существующие RAW
    raw_files = sorted(raw_dir.glob(f"{settings.raw_file_prefix}_*.xlsx"))

    if raw_files:
        latest_raw = raw_files[-1]
        raw_hash = file_hash(latest_raw)

        if raw_hash == source_hash:
            logger.info(f"Source unchanged. Using existing RAW: {latest_raw}")
            #print("Source unchanged. Using existing RAW:", latest_raw)
            return latest_raw

    # создаем новый RAW
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_file = raw_dir / f"{settings.raw_file_prefix}_{timestamp}.xlsx"

    #shutil.copy - функция копирования файлов лучше использовать shutil.copy2 тогда копируе и метаданные
    shutil.copy2(source_file, raw_file)

    #print("New RAW file created:", raw_file)
    logger.info(f"New RAW file created: {raw_file}")

    return raw_file


if __name__ == "__main__":
    extract()