from playwright.sync_api import sync_playwright
import time
import sys
import os
import csv
import json
from pathlib import Path
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Настройка stdout для корректного вывода Юникода
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

class OstrovokHotelsDailyParser:
    def __init__(self):
        self.base_url = "https://ostrovok.ru/hotel/russia/western_siberia_irkutsk_oblast_multi/"
        self.api_endpoint = "/hotel/search/v2/site/serp"
        self.region_id = "965821539"  # ID региона для Иркутской области
        self.all_hotels = []
        self.current_dir = Path(__file__).parent
    
    def _run_date(self):
        """Дата запуска: RUN_TZ (например Europe/Moscow) или UTC для консистентности в CI."""
        tz_name = os.environ.get("RUN_TZ", "UTC")
        try:
            return datetime.now(ZoneInfo(tz_name)).date()
        except Exception:
            return date.today()
    
    def get_all_hotels_list(self):
        """Основная функция для парсинга списка отелей на следующие 2 дня"""
        today = self._run_date()
        arrival_date = today + timedelta(days=1)
        departure_date = today + timedelta(days=2)
        search_url = self._build_search_url(arrival_date, departure_date)
        
        print(f"Даты бронирования: {arrival_date.strftime('%d.%m.%Y')} - {departure_date.strftime('%d.%m.%Y')}")
        
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
            
            browser.close()
        
        if self.all_hotels:
            self._deduplicate_hotels()
            self._save_to_csv()
            print(f"\nПарсинг завершён. Всего обработано {len(self.all_hotels)} отелей.")
        else:
            print("\nНе удалось извлечь данные об отелях.")
        
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
                                extracted_hotels = self._extract_hotels_from_json(json_data)
                                if extracted_hotels:
                                    self.all_hotels.extend(extracted_hotels)
                                    print(f"Перехвачено и извлечено {len(extracted_hotels)} отелей. Всего: {len(self.all_hotels)}")
                except Exception as e:
                    print(f"[API] Ошибка разбора ответа: {e}")
        
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
            
            print(f"\n--- Страница {current_page} ---")
            
            def _load_page_and_wait_for_api():
                """Переход на страницу. Без expect_response, чтобы не конфликтовать с page.on('response')."""
                try:
                    page.goto(page_url, wait_until="load", timeout=50000)
                except Exception as e:
                    print(f"[Страница {current_page}] goto: {e}")
                time.sleep(1)
            
            try:
                _load_page_and_wait_for_api()
            except Exception as e:
                print(f"[Страница {current_page}] Загрузка: {e}")
            
            # Дожидаемся появления отелей (обработчик ответа мог сработать чуть позже)
            max_wait_time = 20
            start_time = time.time()
            while len(self.all_hotels) == hotels_before and (time.time() - start_time) < max_wait_time:
                time.sleep(0.4)
                if len(self.all_hotels) > hotels_before:
                    break
            
            hotels_added = len(self.all_hotels) - hotels_before
            
            # Повторная попытка той же страницы при 0 отелей (в CI часто срабатывает со 2-го раза)
            if hotels_added == 0:
                time.sleep(2)
                start_time = time.time()
                while len(self.all_hotels) == hotels_before and (time.time() - start_time) < 12:
                    time.sleep(0.4)
                hotels_added = len(self.all_hotels) - hotels_before
            
            # Повторная загрузка при 0 отелей на любой странице (в CI ответ часто приходит со 2–3 попытки)
            retries_left = 2
            while hotels_added == 0 and retries_left > 0:
                retries_left -= 1
                print(f"Повторная загрузка страницы {current_page} (осталось попыток: {retries_left + 1})...")
                try:
                    _load_page_and_wait_for_api()
                except Exception as e:
                    print(f"[Повтор страницы {current_page}] {e}")
                start_time = time.time()
                while len(self.all_hotels) == hotels_before and (time.time() - start_time) < 28:
                    time.sleep(0.5)
                hotels_added = len(self.all_hotels) - hotels_before
            
            # Если 0 отелей — финальное ожидание (в CI ответ API часто приходит с большой задержкой)
            if hotels_added == 0:
                print(f"Ожидание ответа для страницы {current_page} (до 55 с)...")
                start_time = time.time()
                while len(self.all_hotels) == hotels_before and (time.time() - start_time) < 55:
                    time.sleep(0.5)
                hotels_added = len(self.all_hotels) - hotels_before
            
            if hotels_added > 0:
                print(f"Добавлено {hotels_added} отелей со страницы {current_page}. Переход на следующую страницу...")
            else:
                print(f"На странице {current_page} отелей не получено. Конец списка.")
                break
            
            current_page += 1
            time.sleep(1.5)
        
        print(f"\n=== Всего собрано отелей со всех страниц: {len(self.all_hotels)} ===")
    
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
                    "url": url,
                    "rooms_number": str(static_vm.get("rooms_number", ""))
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
            print(f"Убрано дубликатов: {removed}. Уникальных отелей: {len(unique)}")
        self.all_hotels = unique
    
    def _save_to_csv(self):
        """Сохранение списка отелей в CSV файл (tables/hotels/YYYY-MM-DD.csv)"""
        if not self.all_hotels:
            return
        
        run_date = self._run_date()
        output_dir = self.current_dir / 'tables' / 'hotels'
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_filename = output_dir / f'{run_date.isoformat()}.csv'
        
        fieldnames = ['city', 'ota_hotel_id', 'master_id', 'name', 'name_en', 'address', 'url', 'rooms_number']
        
        try:
            with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                for hotel in self.all_hotels:
                    writer.writerow(hotel)
            print(f"Сохранено {len(self.all_hotels)} отелей в {csv_filename}")
        except Exception as e:
            print(f"Ошибка при сохранении CSV: {e}")
    
if __name__ == "__main__":
    parser = OstrovokHotelsDailyParser()
    parser.get_all_hotels_list()