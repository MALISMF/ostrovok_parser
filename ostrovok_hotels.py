from playwright.sync_api import sync_playwright
import time
import sys
import os
import csv
import logging
from pathlib import Path
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from log_config import setup_logging, get_log_file_path, send_telegram_summary
from ostrovok_api_watcher import OstrovokApiWatcher

# Настройка stdout для корректного вывода Юникода и сброс буфера в CI
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(line_buffering=True)

logger = logging.getLogger(__name__)


def _is_ci():
    return os.environ.get("GITHUB_ACTIONS") == "true" or os.environ.get("CI") == "true"


class OstrovokHotelsDailyParser:
    def __init__(self):
        self.base_url = "https://ostrovok.ru/hotel/russia/western_siberia_irkutsk_oblast_multi/"
        self.api_endpoint = "/hotel/search/v2/site/serp"
        self.region_id = "965821539"  # ID региона для Иркутской области
        self.all_hotels = []
        self.current_dir = Path(__file__).parent
        self.ci = _is_ci()
        self.watcher = OstrovokApiWatcher(
            self.current_dir / 'ostrovok_schema.json',
            self.current_dir / 'debug',
        )
        self._response_sample = None  # образец ответа API для обновления схемы после прогона
        self._schema_compared = False  # флаг — схема уже сравнивалась в этом прогоне
        if self.ci:
            logger.info("Режим CI: увеличенные таймауты и ожидание networkidle.")
    
    def _run_date(self):
        """Дата запуска по RUN_TZ (по умолчанию Asia/Irkutsk)."""
        tz_name = os.environ.get("RUN_TZ", "Asia/Irkutsk")
        try:
            return datetime.now(ZoneInfo(tz_name)).date()
        except Exception:
            return date.today()
    
    def get_all_hotels_list(self):
        """Основная функция для парсинга списка отелей на следующие 2 дня"""
        logger.info("Запуск парсера отелей...")
        today = self._run_date()
        arrival_date = today + timedelta(days=1)
        departure_date = today + timedelta(days=2)
        search_url = self._build_search_url(arrival_date, departure_date)
        
        logger.info("Даты бронирования: %s - %s", arrival_date.strftime('%d.%m.%Y'), departure_date.strftime('%d.%m.%Y'))
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale='ru-RU',
                viewport={"width": 1920, "height": 1080}
            )
            page = context.new_page()
            
            self._setup_response_interceptor(page)
            self._parse_all_pages_with_pagination(page, search_url)
            # Даём время запоздалым ответам API прийти до закрытия (в CI дольше)
            time.sleep(15 if self.ci else 15)
            browser.close()
        
        if self.all_hotels:
            if self._response_sample is not None:
                self.watcher.save_working_schema(self._response_sample, self.api_endpoint)
            self._deduplicate_hotels()
            self._save_to_csv()
            logger.info("Парсинг завершён. Всего обработано %s отелей.", len(self.all_hotels))
        else:
            logger.warning("Не удалось извлечь данные об отелях.")
        
        return self.all_hotels
            
    def _build_search_url(self, arrival_date, departure_date):
        """Построение URL поиска для CSV столбца show_rooms_url"""
        dates_str = f"{arrival_date.strftime('%d.%m.%Y')}-{departure_date.strftime('%d.%m.%Y')}"
        url = (
            f"{self.base_url}"
            f"?type_group=hotel"
            f"&search=yes"
            f"&dates={dates_str}"
            f"&guests=1"
            f"&q={self.region_id}"
        )
        return url
    
    def _setup_response_interceptor(self, page):
        """Перехват ответов от API Ostrovok"""
        def handle_response(response):
            # Основной эндпоинт
            if (response.request.method == "POST" and 
                self.api_endpoint in response.url and
                response.status == 200 and
                "session=" in response.url):
                
                try:
                    if "json" in response.headers.get("content-type", "").lower():
                        json_data = response.json()
                        
                        if isinstance(json_data, dict) and "hotels" in json_data:
                            hotels = json_data.get("hotels")
                            if hotels and isinstance(hotels, list) and len(hotels) > 0:
                                if not self._schema_compared:
                                    self.watcher.compare_to_schema(json_data, response.url)
                                    self._schema_compared = True
                                self._response_sample = {**json_data, 'hotels': hotels[:1]}
                                extracted_hotels = self._extract_hotels_from_json(json_data)
                                if extracted_hotels:
                                    self.all_hotels.extend(extracted_hotels)
                                    logger.info("Перехвачено и извлечено %s отелей. Всего: %s", len(extracted_hotels), len(self.all_hotels))
                except Exception as e:
                    msg = str(e)
                    if ("No resource with given identifier" not in msg and "getResponseBody" not in msg
                            and "Target page, context or browser has been closed" not in msg):
                        logger.error("Ошибка разбора ответа API: %s", e)
                return

            # Все остальные POST-запросы, кандидаты на новый эндпоинт
            if (response.request.method == "POST" and
                    response.status == 200 and
                    self.api_endpoint not in response.url):
                self.watcher.check_candidate_endpoint(response)
        
        page.on("response", handle_response)
    
    def _parse_all_pages_with_pagination(self, page, base_search_url):
        """Парсинг всех страниц с пагинацией"""
        current_page = 1
        max_pages = 100
        
        while current_page <= max_pages:
            hotels_before = len(self.all_hotels)
            
            if current_page == 1:
                page_url = base_search_url
            else:
                page_url = self._add_page_to_url(base_search_url, current_page)
            
            logger.info("--- Страница %s ---", current_page)
            
            goto_timeout = 90000 if self.ci else 90000
            
            def _load_page_and_wait_for_api():
                """Переход на страницу + в CI ожидание networkidle (все запросы страницы завершены)."""
                try:
                    page.goto(page_url, wait_until="load", timeout=goto_timeout)
                except Exception as e:
                    logger.warning("[Страница %s] goto: %s", current_page, e)
                if self.ci:
                    try:
                        page.wait_for_load_state("networkidle", timeout=45000)
                    except Exception:
                        pass
                time.sleep(2 if self.ci else 3)
            
            try:
                _load_page_and_wait_for_api()
            except Exception as e:
                logger.warning("[Страница %s] Загрузка: %s", current_page, e)
            
            # Дожидаемся появления отелей (в CI дольше — медленная сеть)
            max_wait_time = 45 if self.ci else 40
            start_time = time.time()
            while len(self.all_hotels) == hotels_before and (time.time() - start_time) < max_wait_time:
                time.sleep(0.5)
                if len(self.all_hotels) > hotels_before:
                    break
            hotels_added = len(self.all_hotels) - hotels_before
            
            if hotels_added > 0:
                logger.info("Добавлено %s отелей со страницы %s. Переход на следующую страницу...", hotels_added, current_page)
            else:
                logger.warning("На странице %s отелей не получено. Конец списка.", current_page)
                break
            
            current_page += 1
            time.sleep(2.5 if self.ci else 3)
        
        logger.info("=== Всего собрано отелей со всех страниц: %s ===", len(self.all_hotels))
    
    def _add_page_to_url(self, url, page_number):
        """Добавление номера страницы к URL"""
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        qs["page"] = [str(page_number)]
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parsed._replace(query=new_query))
    
    def _extract_hotels_from_json(self, json_data):
        """Извлечение отелей из JSON ответа от API Ostrovok"""
        hotels_list = []
        
        if not json_data or "hotels" not in json_data:
            return hotels_list
        
        hotels_array = json_data["hotels"]
        
        if hotels_array is None:
            return hotels_list
        
        if not isinstance(hotels_array, list):
            if isinstance(hotels_array, dict):
                hotels_array = [hotels_array]
            else:
                return hotels_list
        
        for hotel in hotels_array:
            try:
                static_vm = hotel.get("static_vm", {})
                if not static_vm:
                    continue
                
                ota_hotel_id = hotel.get("ota_hotel_id", "")
                name = static_vm.get("name", "")
                
                if not ota_hotel_id or not name:
                    continue
                
                master_id = str(hotel.get("master_id") or static_vm.get("master_id", ""))
                url = f"https://ostrovok.ru/hotel/russia/irkutsk/mid{master_id}/{ota_hotel_id}"
                
                hotel_data = {
                    "city": static_vm.get("city", ""),
                    "ota_hotel_id": ota_hotel_id,
                    "master_id": master_id,
                    "name": name,
                    "name_en": static_vm.get("name_en", ""),
                    "address": static_vm.get("address", ""),
                    "latitude": str(static_vm.get("latitude", "")),
                    "longitude": str(static_vm.get("longitude", "")),
                    "url": url,
                    "rooms_number": str(static_vm.get("rooms_number", "")),
                }
                
                hotels_list.append(hotel_data)
            except Exception as e:
                continue
        
        return hotels_list
    
    def _deduplicate_hotels(self):
        """Удаление дубликатов по (ota_hotel_id, master_id), порядок сохраняется."""
        seen = set()
        unique = []
        for h in self.all_hotels:
            key = (h.get("ota_hotel_id") or "", h.get("master_id") or "")
            if key not in seen:
                seen.add(key)
                unique.append(h)
        removed = len(self.all_hotels) - len(unique)
        if removed:
            logger.info("Убрано дубликатов: %s. Уникальных отелей: %s", removed, len(unique))
        self.all_hotels = unique
    
    def _save_to_csv(self):
        """Сохранение списка отелей в CSV файл (daily/hotels/YYYY-MM-DD.csv)"""
        if not self.all_hotels:
            return
        
        run_date = self._run_date()
        output_dir = self.current_dir / 'daily' / 'hotels'
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_filename = output_dir / f'{run_date.isoformat()}.csv'
        
        fieldnames = [
            'city',
            'ota_hotel_id',
            'master_id',
            'name',
            'name_en',
            'address',
            'latitude',
            'longitude',
            'url',
            'rooms_number'
        ]
        
        try:
            with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                for hotel in self.all_hotels:
                    writer.writerow(hotel)
            logger.info("Сохранено %s отелей в %s", len(self.all_hotels), csv_filename)
        except Exception as e:
            logger.error("Ошибка при сохранении CSV: %s", e)


class OstrovokHotelsCatalog:
    """Ведёт накопленный каталог спарсенных отелей за всё время.
    При каждом запуске — добавляет новые отели и обновляет last_seen_date у существующих.
    Файл: catalog/hotels.csv"""

    FIELDNAMES = [
        'ota_hotel_id', 'master_id', 'name', 'name_en',
        'city', 'address', 'latitude', 'longitude', 'url', 'rooms_number',
        'first_seen_date', 'last_seen_date',
    ]

    def __init__(self):
        self.current_dir = Path(__file__).parent
        self.catalog_path = self.current_dir / 'catalog' / 'hotels.csv'

    def _run_date(self):
        tz_name = os.environ.get("RUN_TZ", "Asia/Irkutsk")
        try:
            return datetime.now(ZoneInfo(tz_name)).date()
        except Exception:
            return date.today()

    def _load_existing(self):
        """Читает текущий каталог."""
        existing = {}
        if not self.catalog_path.exists():
            return existing
        try:
            with open(self.catalog_path, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    hotel_id = row.get('ota_hotel_id', '')
                    if hotel_id:
                        existing[hotel_id] = row
        except Exception as e:
            logger.error("Ошибка при чтении каталога %s: %s", self.catalog_path, e)
        return existing

    def _save(self, hotels: dict):
        """Сохраняет каталог."""
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.catalog_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(hotels.values())
            logger.info("Каталог сохранён: %s отелей → %s", len(hotels), self.catalog_path)
        except Exception as e:
            logger.error("Ошибка при сохранении каталога: %s", e)

    def update(self, parsed_hotels: list):
        """Обновляет каталог на основе последнего списка отелей.
        - Новые отели добавляются с first_seen_date = сегодня.
        - Существующие — обновляют поля и last_seen_date."""
        today = self._run_date().isoformat()
        existing = self._load_existing()

        new_count = 0
        for hotel in parsed_hotels:
            hotel_id = hotel.get('ota_hotel_id', '')
            if not hotel_id:
                continue

            # Обновляем поля и дату последнего появления
            if hotel_id in existing:
                existing[hotel_id].update({
                    'name':         hotel.get('name', existing[hotel_id]['name']),
                    'name_en':      hotel.get('name_en', existing[hotel_id]['name_en']),
                    'city':         hotel.get('city', existing[hotel_id]['city']),
                    'address':      hotel.get('address', existing[hotel_id]['address']),
                    'latitude':     hotel.get('latitude', existing[hotel_id].get('latitude', '')),
                    'longitude':    hotel.get('longitude', existing[hotel_id].get('longitude', '')),
                    'url':          hotel.get('url', existing[hotel_id]['url']),
                    'rooms_number': hotel.get('rooms_number', existing[hotel_id]['rooms_number']),
                    'last_seen_date': today,
                })
            # Новый отель
            else:
                existing[hotel_id] = {
                    'ota_hotel_id':   hotel_id,
                    'master_id':      hotel.get('master_id', ''),
                    'name':           hotel.get('name', ''),
                    'name_en':        hotel.get('name_en', ''),
                    'city':           hotel.get('city', ''),
                    'address':        hotel.get('address', ''),
                    'latitude':       hotel.get('latitude', ''),
                    'longitude':      hotel.get('longitude', ''),
                    'url':            hotel.get('url', ''),
                    'rooms_number':   hotel.get('rooms_number', ''),
                    'first_seen_date': today,
                    'last_seen_date':  today,
                }
                new_count += 1

        self._save(existing)
        logger.info("Каталог обновлён: всего %s, новых %s", len(existing), new_count)
        return len(existing), new_count


def _run_date_for_log():
    tz_name = os.environ.get("RUN_TZ", "Asia/Irkutsk")
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        return date.today()


if __name__ == "__main__":
    run_date = _run_date_for_log()
    setup_logging(log_file=get_log_file_path(run_date))

    parser = OstrovokHotelsDailyParser()
    result = parser.get_all_hotels_list()
    
    catalog = OstrovokHotelsCatalog()
    total, new_count = catalog.update(result)

    send_telegram_summary(
        f"Ostrovok: парсинг отелей завершён. Отелей: {len(result)}."
        f"Каталог: {total} всего, {new_count} новых. Дата: {run_date}."
    )