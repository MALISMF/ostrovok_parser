import sys
from typing import List

# Настройка stdout для корректного вывода Юникода (на случай запуска файла напрямую)
if sys.stdout.encoding != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def compute_max_capacity(room_name: str, beds_list: List[str]) -> int:
    """
    Оценивает максимальную вместимость (кол-во гостей) для одного номера.

    Логика:
    1) По тексту названия:
       - "одноместн"  -> 1
       - "двухместн"  -> 2
       - "трехместн"/"трёхместн" -> 3
       - "четырехместн"/"четырёхместн" -> 4
       - "семейн" (семейный номер) -> 3
    2) По списку beds:
       - содержит "двуспальн"           -> +2
       - содержит "2 отдельные кровати" -> +2
       - содержит "диван"               -> +1
       - содержит "семейн"              -> +3

    В итоге берём максимум из оценки по названию и по beds.
    """

    name = (room_name or "").lower()
    beds_list = beds_list or []

    # 1) Вместимость по названию
    name_capacity = 0
    if "одноместн" in name:
        name_capacity = max(name_capacity, 1)
    if "двухместн" in name:
        name_capacity = max(name_capacity, 2)
    if "трехместн" in name or "трёхместн" in name:
        name_capacity = max(name_capacity, 3)
    if "четырехместн" in name or "четырёхместн" in name:
        name_capacity = max(name_capacity, 4)
    # Семейный номер
    if "семейн" in name:
        name_capacity = max(name_capacity, 3)

    # 2) Вместимость по beds
    beds_capacity = 0
    for bed in beds_list:
        s = (bed or "").lower()

        # Семейная кровать/кровать семейного типа
        if "семейн" in s:
            beds_capacity += 3

        # Двуспальная кровать
        if "двуспальн" in s:
            beds_capacity += 2

        # 2 отдельные кровати / две отдельные кровати
        if "2 отдельные кровати" in s or "две отдельные кровати" in s:
            beds_capacity += 2

        # Диван / диван-кровать
        if "диван" in s:
            beds_capacity += 1

    # Итог: используем максимум из оценки по названию и по beds
    capacity = beds_capacity if beds_capacity > 0 else name_capacity
    return capacity if capacity > 0 else 1

