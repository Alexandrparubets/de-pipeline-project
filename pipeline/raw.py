import hashlib
import shutil
import re
from datetime import datetime
from pathlib import Path

from pipeline.config import settings
from pipeline.logger_config import get_logger


logger = get_logger("pipeline.raw")


def create_raw_copy(source_file: Path, pipeline_name: str) -> tuple[Path, str]:
    """
    Creates RAW copy of source file without changing content.

    Returns:
        tuple[Path, str]:
            - path to RAW file
            - SHA256 hash of RAW file
    """
    raw_dir = Path(settings.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    current_hash = calculate_file_hash(source_file)

    latest_raw_file = find_latest_raw_file(source_file, pipeline_name)

    if latest_raw_file is not None:
        latest_raw_hash = calculate_file_hash(latest_raw_file)

        if latest_raw_hash == current_hash:
            logger.info(
                f"♻️ Source unchanged. Using existing RAW: {latest_raw_file}\n"
            )
            return latest_raw_file, current_hash

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source_name = source_file.stem.lower()
    pipeline_part = normalize_name_for_file(pipeline_name)
    source_suffix = source_file.suffix

    raw_file_name = f"{source_name}__{pipeline_part}__{timestamp}{source_suffix}"
    raw_file_path = raw_dir / raw_file_name

    try:
        shutil.copy2(source_file, raw_file_path)
    except Exception as e:
        logger.error(
            f"Failed to create RAW copy from '{source_file}' to '{raw_file_path}': {e}"
        )
        raise

    file_hash = current_hash

    logger.info(f"📥 RAW copy created: {raw_file_path}\n")

    return raw_file_path, file_hash


def calculate_file_hash(file_path: Path) -> str:
    """
    Calculates SHA256 hash of a file.
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def normalize_name_for_file(name: str) -> str:
    """
    Converts text into safe filename part.
    """
    normalized = name.strip().lower().replace(" ", "_")
    normalized = re.sub(r"[^a-z0-9_]+", "", normalized)
    return normalized


def find_latest_raw_file(source_file: Path, pipeline_name: str) -> Path | None:
    """
    Finds latest RAW file for the given source file and pipeline name.
    """
    raw_dir = Path(settings.raw_dir)

    if not raw_dir.exists():
        return None

    source_name = source_file.stem.lower()
    pipeline_part = normalize_name_for_file(pipeline_name)

    pattern = f"{source_name}__{pipeline_part}__*{source_file.suffix}"

    matching_files = sorted(
        raw_dir.glob(pattern),
        key=lambda p: p.name,
        reverse=True,
    )

    if matching_files:
        return matching_files[0]

    return None