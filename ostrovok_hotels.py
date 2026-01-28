from playwright.sync_api import sync_playwright
import time
import sys
import csv
import json
from pathlib import Path
from datetime import date, timedelta
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
    
    def get_all_hotels_list(self):
        today = date.today()
        arrival_date = today + timedelta(days=1)
        departure_date = today + timedelta(days=2)
        search_url = self._build_search_url(arrival_date, departure_date)
        
        print(f"Даты бронирования: {arrival_date.strftime('%d.%m.%Y')} - {departure_date.strftime('%d.%m.%Y')}")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale='ru-RU'
            )
            page = context.new_page()
            
            self._setup_response_interceptor(page)
            self._parse_all_pages_with_pagination(page, search_url)
            
            browser.close()
        
        if self.all_hotels:
            self._save_to_csv()
            print(f"\nПарсинг завершён. Всего обработано {len(self.all_hotels)} отелей.")
        else:
            print("\nНе удалось извлечь данные об отелях.")
        
        return self.all_hotels
            

    def _build_search_url(self, arrival_date, departure_date):
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
                except Exception:
                    pass
        
        page.on("response", handle_response)
    
    def _parse_all_pages_with_pagination(self, page, base_search_url):
        current_page = 1
        max_pages = 3
        
        while current_page <= max_pages:
            hotels_before = len(self.all_hotels)
            
            if current_page == 1:
                page_url = base_search_url
            else:
                page_url = self._add_page_to_url(base_search_url, current_page)
            
            print(f"\n--- Страница {current_page} ---")
            
            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
            except Exception:
                pass
            
            max_wait_time = 15
            start_time = time.time()
            
            while len(self.all_hotels) == hotels_before and (time.time() - start_time) < max_wait_time:
                time.sleep(0.3)
                if len(self.all_hotels) > hotels_before:
                    break
            
            hotels_added = len(self.all_hotels) - hotels_before
            if hotels_added > 0:
                print(f"Добавлено {hotels_added} отелей со страницы {current_page}. Переход на следующую страницу...")
            
            next_page = current_page + 1
            next_link = page.locator(f'a:has-text("{next_page}")').first
            
            if next_link.count() == 0:
                print(f"Ссылка на страницу {next_page} не найдена. Достигнута последняя страница.")
                break
            
            current_page += 1
            time.sleep(0.5)
        
        print(f"\n=== Всего собрано отелей со всех страниц: {len(self.all_hotels)} ===")
    
    def _add_page_to_url(self, url, page_number):
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        qs["page"] = [str(page_number)]
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parsed._replace(query=new_query))
    
    def _extract_hotels_from_json(self, json_data):
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
    
    def _save_to_csv(self):
        if not self.all_hotels:
            return
        
        output_dir = self.current_dir / 'output'
        output_dir.mkdir(exist_ok=True)
        csv_filename = output_dir / 'ostrovok_hotels.csv'
        
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