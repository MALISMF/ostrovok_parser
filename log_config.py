"""Общая настройка логирования: консоль + файл (по желанию). Папка логов — logs в корне проекта."""
import logging
import os
import sys
from pathlib import Path

# Папка логов в корне проекта (рядом с log_config.py)
LOGS_DIR = Path(__file__).resolve().parent / "logs"


def get_log_file_path(run_date):
    """Путь к файлу лога за указанную дату: logs/YYYY-MM-DD.log в корне проекта."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"{run_date}.log"


def setup_logging(
    level=None,
    log_file=None,
    format_string="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    date_fmt="%Y-%m-%d %H:%M:%S",
):
    level = level or os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level, logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_file = os.fspath(log_file)
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setFormatter(logging.Formatter(format_string, datefmt=date_fmt))
        handlers.append(fh)

    logging.basicConfig(
        level=level,
        format=format_string,
        datefmt=date_fmt,
        handlers=handlers,
        force=True,
    )
