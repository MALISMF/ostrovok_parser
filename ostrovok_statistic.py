import csv
import sys
from pathlib import Path
from datetime import date
from collections import defaultdict

# Настройка stdout для корректного вывода Юникода
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

def generate_statistics(run_date=None):
    """Генерирует статистику по отелям на основе данных из CSV файлов.
    run_date — дата сбора (по умолчанию сегодня). Файлы: tables/hotels/{date}.csv, tables/rooms/{date}.csv → tables/statistics/{date}.csv"""
    
    current_dir = Path(__file__).parent
    if run_date is None:
        run_date = date.today()
    date_str = run_date.isoformat()
    hotels_csv = current_dir / 'tables' / 'hotels' / f'{date_str}.csv'
    rooms_csv = current_dir / 'tables' / 'rooms' / f'{date_str}.csv'
    output_csv = current_dir / 'tables' / 'statistics' / f'{date_str}.csv'
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    # Читаем данные об отелях
    hotels_data = {}
    try:
        with open(hotels_csv, 'r', encoding='utf-8-sig', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ota_hotel_id = row.get('ota_hotel_id', '')
                if ota_hotel_id:
                    hotels_data[ota_hotel_id] = {
                        'name': row.get('name', ''),
                        'rooms_number': row.get('rooms_number', '')
                    }
    except Exception as e:
        print(f"Ошибка при чтении {hotels_csv}: {e}")
        return
    
    # Собираем статистику по номерам
    rooms_stats = defaultdict(lambda: {
        'free_rooms_amount': 0,
        'min_price': None,
        'max_capacity': 0,  # суммарная вместимость всех свободных номеров
    })
    
    try:
        with open(rooms_csv, 'r', encoding='utf-8-sig', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ota_hotel_id = row.get('ota_hotel_id', '')
                if not ota_hotel_id:
                    continue
                
                # Суммируем allotment
                allotment = row.get('allotment', '')
                try:
                    allotment_value = int(allotment) if allotment else 0
                    rooms_stats[ota_hotel_id]['free_rooms_amount'] += allotment_value
                except (ValueError, TypeError):
                    pass

                # Суммарная вместимость свободных номеров (allotment * capacity одного номера)
                max_cap_str = row.get('capacity', '')
                try:
                    capacity_per_room = int(max_cap_str) if max_cap_str else 0
                    if capacity_per_room > 0 and allotment_value > 0:
                        rooms_stats[ota_hotel_id]['max_capacity'] += allotment_value * capacity_per_room
                except (ValueError, TypeError):
                    pass
                
                # Находим минимальную цену
                price_min = row.get('price_rub_min', '')
                if price_min:
                    try:
                        price_value = float(price_min)
                        current_min = rooms_stats[ota_hotel_id]['min_price']
                        if current_min is None or price_value < current_min:
                            rooms_stats[ota_hotel_id]['min_price'] = price_value
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"Ошибка при чтении {rooms_csv}: {e}")
        return
    
    # Формируем итоговые данные
    today = date.today()
    collection_date = today.strftime('%Y-%m-%d')
    
    statistics = []
    
    # Обрабатываем отели из hotels_csv
    for ota_hotel_id, hotel_info in hotels_data.items():
        rooms_num_str = hotel_info.get('rooms_number', '')
        try:
            rooms_num = int(rooms_num_str) if rooms_num_str else 0
        except (ValueError, TypeError):
            rooms_num = 0
        
        stats = rooms_stats.get(ota_hotel_id, {})
        free_rooms_amount = stats.get('free_rooms_amount', 0)
        min_price = stats.get('min_price')
        max_capacity = stats.get('max_capacity', 0)
        
        # Вычисляем процент доступных номеров
        if rooms_num > 0:
            available_rooms_percent = round((free_rooms_amount / rooms_num) * 100, 2)
        else:
            available_rooms_percent = 0.0
        
        # Форматируем минимальную цену
        min_price_str = f"{min_price:.2f}" if min_price is not None else ""
        
        statistics.append({
            'ota_hotel_id': ota_hotel_id,
            'name': hotel_info.get('name', ''),
            'rooms_num': str(rooms_num),
            'free_rooms_amount': str(free_rooms_amount),
            'max_capacity': str(max_capacity),
            'available_rooms_percent': str(available_rooms_percent),
            'date': collection_date,
            'min_price': min_price_str
        })
    
    # Сохраняем в CSV
    fieldnames = [
        'ota_hotel_id',
        'name',
        'rooms_num',
        'free_rooms_amount',
        'max_capacity',
        'available_rooms_percent',
        'date',
        'min_price'
    ]
    
    try:
        with open(output_csv, 'w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(statistics)
        print(f"Статистика сохранена в {output_csv}")
        print(f"Обработано {len(statistics)} отелей")
    except Exception as e:
        print(f"Ошибка при сохранении статистики: {e}")

if __name__ == "__main__":
    generate_statistics()
