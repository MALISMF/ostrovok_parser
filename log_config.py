"""Общая настройка логирования: консоль + файл (по желанию). Папка логов — logs в корне проекта.
Telegram: при TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID ошибки уходят в Telegram,
итог парсинга — через send_telegram_summary()."""
import logging
import os
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

# Папка логов в корне проекта (рядом с log_config.py)
LOGS_DIR = Path(__file__).resolve().parent / "logs"
TELEGRAM_MESSAGE_MAX_LENGTH = 4096


def get_log_file_path(run_date):
    """Путь к файлу лога за указанную дату: logs/YYYY-MM-DD.log в корне проекта."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"{run_date}.log"


def _send_telegram(text):
    """Отправить одно сообщение в Telegram. Возвращает True при успехе."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id or not requests:
        return False
    text = (text or "").strip()[:TELEGRAM_MESSAGE_MAX_LENGTH]
    if not text:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False


def send_telegram_summary(message):
    """Отправить итог парсинга в Telegram (если заданы TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID)."""
    _send_telegram(message)


class TelegramHandler(logging.Handler):
    """Отправляет в Telegram только записи уровня ERROR."""

    def emit(self, record):
        try:
            msg = self.format(record)
            if msg:
                _send_telegram(f"[Ошибка] {msg}")
        except Exception:
            self.handleError(record)


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

    if os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"):
        th = TelegramHandler()
        th.setLevel(logging.ERROR)
        th.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        handlers.append(th)

    logging.basicConfig(
        level=level,
        format=format_string,
        datefmt=date_fmt,
        handlers=handlers,
        force=True,
    )
