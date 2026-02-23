from playwright.sync_api import sync_playwright
import time
import sys
import os
import json
import csv
import uuid
import logging
import requests
from pathlib import Path
from urllib.parse import urlparse
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from capacity_utils import compute_max_capacity
from log_config import setup_logging, get_log_file_path, send_telegram_summary

# Настройка stdout для корректного вывода Юникода
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

logger = logging.getLogger(__name__)

class OstrovokRoomsDailyParser:
    def __init__(self):
        self.session = requests.Session()
        self.api_url = "https://ostrovok.ru/hotel/search/v1/site/hp/search"
        self.cookies = None
        self.current_dir = Path(__file__).parent
    
    def _run_date(self):
        """Дата запуска по RUN_TZ (по умолчанию Asia/Irkutsk)."""
        tz_name = os.environ.get("RUN_TZ", "Asia/Irkutsk")
        try:
            return datetime.now(ZoneInfo(tz_name)).date()
        except Exception:
            return date.today()
    
    def _get_cookies_from_browser(self):
        """Получение куки через реальный браузер"""
        logger.info("Запуск браузера для получения куки...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            page = context.new_page()
            page.goto('https://ostrovok.ru')
            
            # Получаем куки
            cookies = context.cookies()
            self.cookies = {cookie['name']: cookie['value'] for cookie in cookies}
            
            browser.close()
            
        logger.info("Получено %s куки", len(self.cookies))
        return self.cookies

    def _extract_hotel_id(self, hotel_url):
        """Достаем url-идентификатор отеля из URL (последний сегмент пути)."""
        try:
            path = urlparse(hotel_url).path.rstrip("/")
            return path.split("/")[-1] if path else None
        except Exception:
            return None
    
    def _search_hotel(self, hotel_id, arrival_date, departure_date, adults=1):
        """Запрос данных по отелю через API Ostrovok"""
        
        if not self.cookies:
            self._get_cookies_from_browser()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Origin': 'https://ostrovok.ru',
            'Referer': 'https://ostrovok.ru/'
        }
        
        payload = {
            "arrival_date": arrival_date.strftime("%Y-%m-%d"),
            "departure_date": departure_date.strftime("%Y-%m-%d"),
            "hotel": hotel_id,
            "currency": "RUB",
            "lang": "ru",
            "region_id": 965821539,
            "paxes": [{"adults": adults}],
            "search_uuid": str(uuid.uuid4())
        }
        
        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers,
                cookies=self.cookies,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning("Ошибка: %s", response.status_code)
                return None
                
        except Exception:
            return None

    def _extract_room_data(self, json_data):
        """Извлекает данные по каждому номеру из JSON ответа API Ostrovok и группирует по rg_hash"""
        rooms_by_rg_hash = {}
        
        hotel_id = json_data.get("ota_hotel_id", "")
        master_id = str(json_data.get("master_id", ""))
        rates = json_data.get("rates", [])
        base_hotel_url = "https://ostrovok.ru/hotel/russia/western_siberia_irkutsk_oblast_multi/"
        hotel_url = f"{base_hotel_url}mid{master_id}/{hotel_id}"
        
        if not rates:
            # Если по отелю не пришли rates, всё равно пишем строку по отелю (для контроля пропусков)
            return [{
                "ota_hotel_id": hotel_id,
                "master_id": master_id,
                "room_name": "",
                "rg_hash": "",
                "count_rg_hash": "0",
                "allotment": "",
                "bedding_type": "",
                "beds": "",
                "bedding_data": "",
                "multi_bed_data": "",
                "capacity": "",
                "price_rub_min": "",
                "price_rub_max": "",
                "url": hotel_url,
            }]
        
        for rate in rates:
            payment_options = rate.get("payment_options", {})
            payment_types_list = payment_options.get("payment_types", [])
            price_rub = ""
            if payment_types_list:
                first_payment = payment_types_list[0]
                price_rub = first_payment.get("amount") or first_payment.get("show_amount", "")
            
            # Преобразуем цену в число для сравнения
            try:
                price_value = float(price_rub) if price_rub else float('inf')
            except (ValueError, TypeError):
                price_value = float('inf')
            
            rooms = rate.get("rooms", [])
            
            if not rooms:
                # Если нет rooms, используем данные из rate
                rg_hash = ""
                room_name = rate.get("room_name", "")
                room_data_trans = rate.get("room_data_trans", {}).get("ru", {})
                bedding_type = room_data_trans.get("bedding_type", "")
                beds_list = room_data_trans.get("beds") or []
                beds_str = json.dumps(beds_list, ensure_ascii=False) if beds_list else ""
                allotment = rate.get("allotment", 0)
                bedding_data = rate.get("bedding_data", [])
                multi_bed_data = rate.get("multi_bed_data", [])

                # Вместимость одного номера
                capacity_per_room = compute_max_capacity(room_name, beds_list)
                
                # Пропускаем записи без rg_hash
                if not rg_hash:
                    continue
                
                # Преобразуем allotment в число
                try:
                    allotment_value = int(allotment) if allotment else 0
                except (ValueError, TypeError):
                    allotment_value = 0
                
                # Преобразуем bedding_data и multi_bed_data в строки
                bedding_data_str = json.dumps(bedding_data, ensure_ascii=False) if bedding_data else ""
                multi_bed_data_str = json.dumps(multi_bed_data, ensure_ascii=False) if multi_bed_data else ""
                
                # Группируем по rg_hash
                if rg_hash in rooms_by_rg_hash:
                    # Объединяем: обновляем min/max цены и счетчик
                    existing = rooms_by_rg_hash[rg_hash]
                    existing["count_rg_hash"] += 1
                    if price_value < existing.get("_price_min", float('inf')):
                        existing["price_rub_min"] = price_rub
                        existing["_price_min"] = price_value
                    if price_value > existing.get("_price_max", float('-inf')):
                        existing["price_rub_max"] = price_rub
                        existing["_price_max"] = price_value
                else:
                    # Первая запись для этого rg_hash
                    rooms_by_rg_hash[rg_hash] = {
                        "ota_hotel_id": hotel_id,
                        "master_id": master_id,
                        "room_name": room_name,
                        "rg_hash": rg_hash,
                        "count_rg_hash": 1,
                        "allotment": str(allotment_value),
                        "bedding_type": bedding_type,
                        "beds": beds_str,
                        "bedding_data": bedding_data_str,
                        "multi_bed_data": multi_bed_data_str,
                        "capacity": str(capacity_per_room),
                        "price_rub_min": price_rub,
                        "price_rub_max": price_rub,
                        "url": hotel_url,
                        "_price_min": price_value,
                        "_price_max": price_value
                    }
            else:
                for room in rooms:
                    rg_hash = room.get("rg_hash", "")
                    room_name = room.get("room_name", "")
                    room_data_trans = room.get("room_data_trans", {}).get("ru", {})
                    bedding_type = room_data_trans.get("bedding_type", "")
                    beds_list = room_data_trans.get("beds") or []
                    beds_str = json.dumps(beds_list, ensure_ascii=False) if beds_list else ""
                    allotment = room.get("allotment", 0)
                    bedding_data = room.get("bedding_data", [])
                    multi_bed_data = room.get("multi_bed_data", [])

                    # Вместимость одного номера
                    capacity_per_room = compute_max_capacity(room_name, beds_list)
                    
                    # Пропускаем записи без rg_hash
                    if not rg_hash:
                        continue
                    
                    # Преобразуем allotment в число
                    try:
                        allotment_value = int(allotment) if allotment else 0
                    except (ValueError, TypeError):
                        allotment_value = 0
                    
                    # Преобразуем bedding_data и multi_bed_data в строки
                    bedding_data_str = json.dumps(bedding_data, ensure_ascii=False) if bedding_data else ""
                    multi_bed_data_str = json.dumps(multi_bed_data, ensure_ascii=False) if multi_bed_data else ""
                    
                    # Группируем по rg_hash
                    if rg_hash in rooms_by_rg_hash:
                        # Объединяем: обновляем min/max цены и счетчик
                        existing = rooms_by_rg_hash[rg_hash]
                        existing["count_rg_hash"] += 1
                        if price_value < existing.get("_price_min", float('inf')):
                            existing["price_rub_min"] = price_rub
                            existing["_price_min"] = price_value
                        if price_value > existing.get("_price_max", float('-inf')):
                            existing["price_rub_max"] = price_rub
                            existing["_price_max"] = price_value
                    else:
                        # Первая запись для этого rg_hash
                        rooms_by_rg_hash[rg_hash] = {
                            "ota_hotel_id": hotel_id,
                            "master_id": master_id,
                            "room_name": room_name,
                            "rg_hash": rg_hash,
                            "count_rg_hash": 1,
                            "allotment": str(allotment_value),
                            "bedding_type": bedding_type,
                            "beds": beds_str,
                            "bedding_data": bedding_data_str,
                            "multi_bed_data": multi_bed_data_str,
                            "capacity": str(capacity_per_room),
                            "price_rub_min": price_rub,
                            "price_rub_max": price_rub,
                            "url": hotel_url,
                            "_price_min": price_value,
                            "_price_max": price_value
                        }
        
        # Удаляем служебные поля и преобразуем count_rg_hash в строку
        rooms_data = []
        for room in rooms_by_rg_hash.values():
            room.pop("_price_min", None)
            room.pop("_price_max", None)
            room["count_rg_hash"] = str(room["count_rg_hash"])
            rooms_data.append(room)
        
        return rooms_data

    def _read_hotels_from_csv(self, csv_path):
        """Читает список отелей из CSV файла"""
        hotels = []
        
        try:
            with open(csv_path, newline="", encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=",")
                for row in reader:
                    hotels.append(row)
        except Exception:
            pass
        
        return hotels

    def _process_hotel(self, hotel_row, arrival_date, departure_date):
        """Обрабатывает один отель: извлекает ID, запрашивает данные"""
        hotel_url = hotel_row.get("show_rooms_url") or hotel_row.get("url") or hotel_row.get("detail_url")
        hotel_name = hotel_row.get("ota_hotel_id") or hotel_row.get("name") or "unknown"
        hotel_id = self._extract_hotel_id(hotel_url) if hotel_url else None

        if not hotel_id:
            logger.warning("Пропускаю %s: не найден hotel_id", hotel_name)
            return []

        logger.info("Запрашиваю %s (%s)", hotel_name, hotel_id)
        result = self._search_hotel(hotel_id, arrival_date, departure_date)

        if not result:
            logger.warning("Нет данных для %s", hotel_name)
            return []

        rooms_data = self._extract_room_data(result)
        return rooms_data

    def get_all_rooms(self, csv_path=None):
        """Основная функция для парсинга номеров отелей из списка"""
        today = self._run_date()
        arrival_date = today + timedelta(days=1)
        departure_date = today + timedelta(days=2)
        
        logger.info("Даты бронирования: %s - %s", arrival_date.strftime('%d.%m.%Y'), departure_date.strftime('%d.%m.%Y'))
        
        if csv_path is None:
            csv_path = self.current_dir / 'tables' / 'hotels' / f'{today.isoformat()}.csv'
        else:
            csv_path = Path(csv_path)
        
        # Получаем куки
        self._get_cookies_from_browser()

        # Читаем список отелей
        hotels = self._read_hotels_from_csv(csv_path)
        
        if not hotels:
            logger.warning("Не удалось загрузить список отелей.")
            return []
        
        # Обрабатываем каждый отель
        all_rooms_data = []
        for hotel_row in hotels:
            rooms_data = self._process_hotel(hotel_row, arrival_date, departure_date)
            if rooms_data:
                all_rooms_data.extend(rooms_data)
                # Для вывода считаем только реальные номера (строки-заглушки имеют пустой rg_hash)
                rooms_count = sum(1 for r in rooms_data if r.get("rg_hash"))
                logger.info("Сохранено %s номеров для %s", rooms_count, hotel_row.get('hotel_name') or hotel_row.get('name', 'unknown'))
        
        if all_rooms_data:
            self._save_to_csv(all_rooms_data)
            logger.info("Парсинг завершён. Всего обработано %s номеров.", len(all_rooms_data))
        else:
            logger.warning("Не удалось извлечь данные о номерах.")
        
        return all_rooms_data
    
    def _save_to_csv(self, rooms_data):
        """Сохраняет данные номеров в CSV файл (tables/rooms/YYYY-MM-DD.csv)"""
        if not rooms_data:
            return
        
        run_date = self._run_date()
        output_dir = self.current_dir / 'tables' / 'rooms'
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_filename = output_dir / f'{run_date.isoformat()}.csv'
        
        fieldnames = [
            "ota_hotel_id",
            "master_id",
            "room_name",
            "rg_hash",
            "count_rg_hash",
            "allotment",
            "bedding_type",
            "beds",
            "bedding_data",
            "multi_bed_data",
            "capacity",
            "price_rub_min",
            "price_rub_max",
            "url"
        ]
        
        try:
            with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                for room in rooms_data:
                    writer.writerow(room)
            logger.info("Сохранено %s номеров в %s", len(rooms_data), csv_filename)
        except Exception as e:
            logger.error("Ошибка при сохранении CSV: %s", e)


def _run_date_for_log():
    tz_name = os.environ.get("RUN_TZ", "Asia/Irkutsk")
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        return date.today()


if __name__ == "__main__":
    run_date = _run_date_for_log()
    setup_logging(log_file=get_log_file_path(run_date))

    parser = OstrovokRoomsDailyParser()
    result = parser.get_all_rooms()
    send_telegram_summary(f"Ostrovok: парсинг номеров завершён. Номеров: {len(result)}. Дата: {run_date}.")
