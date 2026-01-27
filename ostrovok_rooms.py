from playwright.sync_api import sync_playwright
import time
import sys
import json
import csv
import os
import uuid
import requests
from pathlib import Path
from urllib.parse import urlparse
from datetime import date, timedelta

# Настройка stdout для корректного вывода Юникода
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Логика дат бронирования
today_date = date.today()
start_date = today_date + timedelta(days=1)
end_date = today_date + timedelta(days=2)

class OstrovokRoomsParser:
    def __init__(self):
        self.session = requests.Session()
        self.api_url = "https://ostrovok.ru/hotel/search/v1/site/hp/search"
        self.cookies = None
    
    def get_cookies_from_browser(self):
        """Получение куки через реальный браузер"""
        print("Запуск браузера для получения куки...")
        
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
            
        print(f"Получено {len(self.cookies)} куки")
        return self.cookies

    def _extract_hotel_id(self, hotel_url):
        """Достаем url-идентификатор отеля из URL (последний сегмент пути)."""
        try:
            path = urlparse(hotel_url).path.rstrip("/")
            return path.split("/")[-1] if path else None
        except Exception as exc:
            print(f"Не удалось распарсить url {hotel_url}: {exc}")
            return None
    
    def search_hotel(self, hotel_id, checkin_date, checkout_date, adults=1):
        """Поиск с куки из браузера"""
        
        if not self.cookies:
            self.get_cookies_from_browser()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Origin': 'https://ostrovok.ru',
            'Referer': 'https://ostrovok.ru/'
        }
        
        payload = {
            "arrival_date": checkin_date,
            "departure_date": checkout_date,
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
                print(f"Ошибка: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Ошибка: {e}")
            return None

    def extract_room_data(self, json_data):
        """Извлекает данные по каждому номеру из JSON ответа API"""
        rooms_data = []
        
        hotel_id = json_data.get("ota_hotel_id", "")
        master_id = json_data.get("master_id", "")
        rates = json_data.get("rates", [])
        
        if not rates:
            return [{
                "hotel_id": hotel_id,
                "master_id": master_id,
                "rate_hash": "",
                "rg_hash": "",
                "multi_bed_data": "",
                "room_name": "",
                "room_type": "",
                "allotment": "",
                "bedding_type": "",
                "main_bed_count": "",
                "extra_bed_count": "",
                "has_breakfast": "",
                "meal_type": "",
                "amenities": "",
                "price_rub": "",
                "payment_types": "",
                "free_cancellation_before": "",
                "cancellation_penalty_percent": "",
                "no_show_penalty": ""
            }]
        
        for rate in rates:
            rate_hash = rate.get("hash", "")
            
            payment_options = rate.get("payment_options", {})
            payment_types_list = payment_options.get("payment_types", [])
            price_rub = ""
            if payment_types_list:
                first_payment = payment_types_list[0]
                price_rub = first_payment.get("amount") or first_payment.get("show_amount", "")
            
            allowed_payment_types = payment_options.get("allowed_payment_types", [])
            payment_types_str = ", ".join([
                f"{pt.get('type', '')}/{pt.get('by', '')}" 
                for pt in allowed_payment_types
            ])
            
            cancellation_info = rate.get("cancellation_info", {})
            free_cancellation_before = cancellation_info.get("free_cancellation_before", "")
            if free_cancellation_before:
                free_cancellation_before = free_cancellation_before.split("T")[0]
            
            cancellation_policies = cancellation_info.get("policies", [])
            cancellation_penalty_percent = ""
            if cancellation_policies:
                for policy in cancellation_policies:
                    penalty = policy.get("penalty", {})
                    if penalty.get("percent"):
                        cancellation_penalty_percent = penalty.get("percent", "")
                        break
            
            no_show = rate.get("no_show", {})
            no_show_penalty = ""
            if no_show:
                no_show_penalty_obj = no_show.get("penalty", {})
                no_show_penalty = no_show_penalty_obj.get("amount", "")
            
            rooms = rate.get("rooms", [])
            
            if not rooms:
                rooms_data.append({
                    "hotel_id": hotel_id,
                    "master_id": master_id,
                    "rate_hash": rate_hash,
                    "rg_hash": "",
                    "multi_bed_data": "",
                    "room_name": rate.get("room_name", ""),
                    "room_type": rate.get("room_data_trans", {}).get("ru", {}).get("main_room_type", ""),
                    "allotment": rate.get("allotment", ""),
                    "bedding_type": rate.get("room_data_trans", {}).get("ru", {}).get("bedding_type", ""),
                    "main_bed_count": rate.get("bed_places", {}).get("main_count", ""),
                    "extra_bed_count": rate.get("bed_places", {}).get("extra_count", ""),
                    "has_breakfast": rate.get("meal_data", {}).get("meals", [{}])[0].get("has_breakfast", False),
                    "meal_type": rate.get("meal", [""])[0] if rate.get("meal") else "",
                    "amenities": ", ".join(rate.get("serp_filters", [])),
                    "price_rub": price_rub,
                    "payment_types": payment_types_str,
                    "free_cancellation_before": free_cancellation_before,
                    "cancellation_penalty_percent": cancellation_penalty_percent,
                    "no_show_penalty": no_show_penalty
                })
            else:
                for room in rooms:
                    room_name = room.get("room_name", "")
                    room_data_trans = room.get("room_data_trans", {}).get("ru", {})
                    room_type = room_data_trans.get("main_room_type", "")
                    bedding_type = room_data_trans.get("bedding_type", "")
                    
                    bed_places = room.get("bed_places", {})
                    main_bed_count = bed_places.get("main_count", "")
                    extra_bed_count = bed_places.get("extra_count", "")
                    
                    meal_data = room.get("meal_data", {})
                    meals = meal_data.get("meals", [])
                    has_breakfast = False
                    meal_type = ""
                    if meals:
                        has_breakfast = meals[0].get("has_breakfast", False)
                        meal_type = meals[0].get("value", "")
                    
                    if not meal_type:
                        meal_list = room.get("meal", [])
                        if meal_list:
                            meal_type = meal_list[0]
                    
                    serp_filters = room.get("serp_filters", [])
                    amenities = ", ".join(serp_filters) if serp_filters else ""
                    
                    allotment = room.get("allotment", "")
                    rg_hash = room.get("rg_hash", "")
                    multi_bed_data = room.get("multi_bed_data", [])
                    multi_bed_data_str = json.dumps(multi_bed_data, ensure_ascii=False) if multi_bed_data else ""
                    
                    rooms_data.append({
                        "hotel_id": hotel_id,
                        "master_id": master_id,
                        "rate_hash": rate_hash,
                        "rg_hash": rg_hash,
                        "multi_bed_data": multi_bed_data_str,
                        "room_name": room_name,
                        "room_type": room_type,
                        "allotment": allotment,
                        "bedding_type": bedding_type,
                        "main_bed_count": main_bed_count,
                        "extra_bed_count": extra_bed_count,
                        "has_breakfast": "Да" if has_breakfast else "Нет",
                        "meal_type": meal_type,
                        "amenities": amenities,
                        "price_rub": price_rub,
                        "payment_types": payment_types_str,
                        "free_cancellation_before": free_cancellation_before,
                        "cancellation_penalty_percent": cancellation_penalty_percent,
                        "no_show_penalty": no_show_penalty
                    })
        
        return rooms_data

    def read_hotels_from_csv(self, csv_path):
        """Читает список отелей из CSV файла"""
        hotels = []
        
        with open(csv_path, newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile, delimiter=",")
            for row in reader:
                hotels.append(row)
        
        return hotels

    def process_hotel(self, hotel_row, checkin_date, checkout_date):
        """Обрабатывает один отель: извлекает ID, запрашивает данные и сохраняет в CSV"""
        hotel_url = hotel_row.get("show_rooms_url") or hotel_row.get("url") or hotel_row.get("detail_url")
        hotel_name = hotel_row.get("ota_hotel_id") or hotel_row.get("name") or "unknown"
        hotel_id = self._extract_hotel_id(hotel_url) if hotel_url else None

        if not hotel_id:
            print(f"Пропускаю {hotel_name}: не найден hotel_id")
            return False

        print(f"Запрашиваю {hotel_name} ({hotel_id})")
        result = self.search_hotel(hotel_id, checkin_date, checkout_date)

        if not result:
            print(f"Нет данных для {hotel_name}")
            return False

        rooms_data = self.extract_room_data(result)
        return rooms_data

    def get_all_rooms(self, csv_path, checkin_date, checkout_date, output_csv):
        """Основная функция для парсинга номеров отелей из списка"""
        
        # --- Получаем куки ---
        self.get_cookies_from_browser()

        # --- Инициализируем CSV файл ---
        fieldnames = [
            "hotel_id",
            "master_id",
            "rate_hash",
            "rg_hash",
            "multi_bed_data",
            "room_name",
            "room_type",
            "allotment",
            "bedding_type",
            "main_bed_count",
            "extra_bed_count",
            "has_breakfast",
            "meal_type",
            "amenities",
            "price_rub",
            "payment_types",
            "free_cancellation_before",
            "cancellation_penalty_percent",
            "no_show_penalty"
        ]
        
        file_exists = os.path.exists(output_csv)
        if not file_exists:
            with open(output_csv, "w", newline="", encoding="utf-8-sig") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

        # --- Читаем список отелей ---
        hotels = self.read_hotels_from_csv(csv_path)
        
        # --- Обрабатываем каждый отель ---
        all_rooms_data = []
        for hotel_row in hotels:
            rooms_data = self.process_hotel(hotel_row, checkin_date, checkout_date)
            if rooms_data:
                all_rooms_data.extend(rooms_data)
                print(f"Сохранено {len(rooms_data)} номеров для {hotel_row.get('hotel_name') or hotel_row.get('name', 'unknown')}")

        # --- Сохраняем все данные в CSV ---
        with open(output_csv, "a", newline="", encoding="utf-8-sig") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerows(all_rooms_data)
        
        print(f"\n=== Всего сохранено {len(all_rooms_data)} номеров в {output_csv} ===")
        return all_rooms_data

if __name__ == "__main__":
    parser = OstrovokRoomsParser()
    
    # Используем даты из логики выше
    checkin_date = start_date.strftime("%Y-%m-%d")
    checkout_date = end_date.strftime("%Y-%m-%d")
    
    print(f"Даты бронирования: {checkin_date} - {checkout_date}")
    
    csv_path = r"C:\Users\matve\Desktop\ostrovok_parser\output\ostrovok_hotels.csv"
    output_csv = r"C:\Users\matve\Desktop\ostrovok_parser\output\ostrovok_rooms.csv"
    
    parser.get_all_rooms(csv_path, checkin_date, checkout_date, output_csv)
