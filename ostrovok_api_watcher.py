import json
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from log_config import send_telegram_summary

logger = logging.getLogger(__name__)

DEBUG_RETAIN_DAYS = 7


class OstrovokApiWatcher:
    def __init__(self, schema_path: Path, debug_dir: Path):
        self.schema_path = schema_path
        self.debug_dir = debug_dir
        self.schema = self._load_schema()

    # ------------------------------------------------------------------ #
    #  Схема                                                               #
    # ------------------------------------------------------------------ #

    def _load_schema(self) -> dict:
        """Загружает эталонную схему. При отсутствии файла — возвращает defaults."""
        defaults = {
            'known_endpoint': '/hotel/search/v2/site/serp',
            'response_keys': ['hotels', 'meta'],
            'meta_keys': ['total', 'count', 'totalCount'],
            'hotel_keys': ['ota_hotel_id', 'master_id', 'static_vm'],
            'static_vm_keys': ['name', 'city', 'address', 'latitude', 'longitude', 'rooms_number'],
            'hotel_keywords': ['ota_hotel_id', 'master_id', 'static_vm', 'hotel_id'],
            'keyword_threshold': 2,
            'empty_page_threshold': 2,
        }
        if not self.schema_path.exists():
            logger.warning(
                'Файл схемы %s не найден — используются значения по умолчанию.',
                self.schema_path,
            )
            return defaults
        try:
            with open(self.schema_path, encoding='utf-8') as f:
                loaded = json.load(f)
            for k, v in defaults.items():
                loaded.setdefault(k, v)
            logger.info('Схема загружена из %s', self.schema_path)
            return loaded
        except Exception as e:
            logger.error('Ошибка чтения схемы %s: %s — используются defaults', self.schema_path, e)
            return defaults

    def save_working_schema(self, json_data: dict, endpoint_url: str) -> None:
        """
        После успешного парсинга обновляет остуктурные поля схемы на основе
        фактического ответа. Пороги и keyword-настройки не трогает.
        """
        if not json_data:
            return

        preserved_keys = {'hotel_keywords', 'keyword_threshold', 'consecutive_empty_pages_threshold'}

        updated = dict(self.schema)
        updated['known_endpoint'] = endpoint_url

        # Ключи верхнего уровня
        updated['response_keys'] = list(json_data.keys())

        # Ключи meta
        meta = json_data.get('meta', {})
        if isinstance(meta, dict):
            updated['meta_keys'] = list(meta.keys())

        # Ключи отеля и static_vm
        hotels = json_data.get('hotels', [])
        if isinstance(hotels, list) and hotels:
            sample = hotels[0]
            updated['hotel_keys'] = list(sample.keys())
            static_vm = sample.get('static_vm', {})
            if isinstance(static_vm, dict):
                updated['static_vm_keys'] = list(static_vm.keys())

        # Восстанавливаем настройки, которые не должны меняться автоматически
        for k in preserved_keys:
            if k in self.schema:
                updated[k] = self.schema[k]

        try:
            with open(self.schema_path, 'w', encoding='utf-8') as f:
                json.dump(updated, f, ensure_ascii=False, indent=2)
            self.schema = updated
            logger.info('Рабочая схема обновлена в %s', self.schema_path)
        except Exception as e:
            logger.error('Не удалось сохранить рабочую схему: %s', e)

    # ------------------------------------------------------------------ #
    #  Debug dump                                                          #
    # ------------------------------------------------------------------ #

    def _save_debug_dump(self, url: str, data: dict, reason: str = 'anomaly') -> None:
        """
        Сохраняет сырой JSON-ответ в debug/.
        Для кандидатов на новый эндпоинт добавляет путь API в имя файла.
        """
        self.debug_dir.mkdir(exist_ok=True)
        run_date = datetime.now().strftime('%Y-%m-%d')
        ts = datetime.now().strftime('%H%M%S')

        if reason == 'candidate_endpoint':
            endpoint_slug = urlparse(url).path.strip('/').replace('/', '_')
            filename = self.debug_dir / f'{endpoint_slug}.json'
        else:
            filename = self.debug_dir / f'{run_date}_{ts}_{reason}.json'

        try:
            payload = {'url': url, 'reason': reason, 'data': data}
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            logger.info('Debug dump сохранён: %s', filename)
            self._cleanup_debug_dumps()
        except Exception as e:
            logger.error('Не удалось сохранить debug dump: %s', e)

    def _cleanup_debug_dumps(self) -> None:
        """Удаляет debug-файлы старше DEBUG_RETAIN_DAYS дней."""
        cutoff = datetime.now().timestamp() - DEBUG_RETAIN_DAYS * 86400
        removed = 0
        for f in self.debug_dir.glob('*.json'):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except Exception:
                pass
        if removed:
            logger.info('Удалено устаревших debug-файлов: %d', removed)

    # ------------------------------------------------------------------ #
    #  Schema drift detector                                               #
    # ------------------------------------------------------------------ #

    def compare_to_schema(self, json_data: dict, url: str) -> list:
        """
        Сравнивает структуру ответа с эталоном.
        Возвращает список отклонений (пустой список — норма).
        """
        schema = self.schema
        issues = []

        # Ключи верхнего уровня
        for key in schema['response_keys']:
            if key not in json_data:
                issues.append(f"Пропал ключ верхнего уровня: '{key}'")

        known_top = set(schema['response_keys'])
        for key in json_data:
            if key not in known_top:
                issues.append(f"Новый неизвестный ключ верхнего уровня: '{key}'")

        # Структура отеля
        hotels = json_data.get('hotels')
        if isinstance(hotels, list) and hotels:
            sample = hotels[0]
            for key in schema['hotel_keys']:
                if key not in sample:
                    issues.append(f"Пропал ключ отеля: '{key}'")

            static_vm = sample.get('static_vm')
            if isinstance(static_vm, dict):
                for key in schema['static_vm_keys']:
                    if key not in static_vm:
                        issues.append(f"Пропал ключ static_vm: '{key}'")
            elif 'static_vm' in sample:
                issues.append(
                    f"static_vm изменил тип: ожидался dict, получен {type(static_vm).__name__}"
                )

        if issues:
            summary = '; '.join(issues)
            logger.error('[Schema Drift] %s | URL: %s', summary, url)
            send_telegram_summary(
                f'[Schema Drift] Ostrovok Hotels\n{summary}\nURL: {url}'
            )
            self._save_debug_dump(url, json_data, reason='schema_drift')

        return issues

    # ------------------------------------------------------------------ #
    #  Candidate endpoint detector                                         #
    # ------------------------------------------------------------------ #

    def check_candidate_endpoint(self, response) -> None:
        """
        Проверяет незнакомый POST JSON-ответ на наличие ключевых слов.
        Если найдено достаточно совпадений — логирует URL и сохраняет dump.
        """
        try:
            ct = response.headers.get('content-type', '').lower()
            if 'json' not in ct:
                return
            data = response.json()
            if not isinstance(data, dict):
                return

            data_str = json.dumps(data, ensure_ascii=False)
            keywords = self.schema['hotel_keywords']
            threshold = self.schema['keyword_threshold']
            found = [kw for kw in keywords if kw in data_str]

            if len(found) >= threshold:
                logger.warning(
                    '[Кандидат на новый эндпоинт] URL: %s | Ключевые слова: %s',
                    response.url,
                    found,
                )
                send_telegram_summary(
                    f'[Новый эндпоинт?] Ostrovok Hotels\n'
                    f'URL: {response.url}\n'
                    f'Найдены ключевые слова: {found}\n'
                    f'Проверьте вручную — возможно, API изменился.'
                )
                self._save_debug_dump(response.url, data, reason='candidate_endpoint')
        except Exception:
            pass