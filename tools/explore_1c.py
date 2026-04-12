#!/usr/bin/env python3
"""
Скрипт для исследования OData сервиса 1С.
Выгружает метаданные, структуру документов и примеры коммерческих предложений.
"""

import requests
import json
from datetime import datetime
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth

# Конфигурация
BASE_URL = "http://172.22.0.90/1R88669/1R88669_UT11_bfimz0bdj3/odata/standard.odata"
USERNAME = "павел"
PASSWORD = "1"

# Попытаемся закодировать логин/пароль правильно
auth = HTTPBasicAuth(USERNAME, PASSWORD)
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Отключаем проверку SSL для локального сервера
import urllib3
urllib3.disable_warnings()

print("=" * 80)
print("ИССЛЕДОВАНИЕ OData СЕРВИСА 1С")
print("=" * 80)
print(f"\nПодключение к: {BASE_URL}\n")

# === ТЕСТ 1: Метаданные ===
print("\n1. ПОЛУЧЕНИЕ МЕТАДАННЫХ")
print("-" * 80)

try:
    metadata_url = urljoin(BASE_URL, "$metadata")
    response = requests.get(metadata_url, auth=auth, timeout=10, verify=False)
    response.raise_for_status()
    
    print(f"✓ Метаданные получены (статус {response.status_code})")
    
    # Парсим XML метаданных и ищем EntitySet'ы
    metadata_text = response.text
    
    # Простой поиск EntitySet в XML
    import re
    entity_sets = re.findall(r'EntitySet Name="([^"]*)"', metadata_text)
    
    if entity_sets:
        print(f"\nНайдено {len(entity_sets)} объектов в OData:")
        for entity in sorted(entity_sets):
            print(f"  - {entity}")
    else:
        print("Не удалось распарсить объекты из метаданных")
        
except Exception as e:
    print(f"✗ Ошибка при получении метаданных: {e}")

# === ТЕСТ 2: Корневой endpoint ===
print("\n\n2. ТЕСТ КОРНЕВОГО ENDPOINT'а")
print("-" * 80)

try:
    response = requests.get(BASE_URL, auth=auth, timeout=10, verify=False)
    response.raise_for_status()
    
    print(f"✓ Корневой endpoint доступен (статус {response.status_code})")
    
    # Пытаемся распарсить JSON
    try:
        data = response.json()
        if 'value' in data:
            print(f"Получено {len(data['value'])} объектов")
    except:
        # Если JSON не парсится, просто показываем первые 500 char
        print(f"Ответ (первые 500 символов):\n{response.text[:500]}")
        
except Exception as e:
    print(f"✗ Ошибка: {e}")

# === ТЕСТ 3: Поиск коммерческих предложений ===
print("\n\n3. ПОИСК КОММЕРЧЕСКИХ ПРЕДЛОЖЕНИЙ")
print("-" * 80)

# Варианты названий документа коммерческого предложения
kp_variants = [
    "Document_КоммерческоеПредложениеКлиенту",
    "Document_КП",
    "Document_КоммерческоеПредложение",
    "Document_CommercialProposal",
    "КоммерческоеПредложениеКлиенту",
    "КоммерческоеПредложение",
]

found_kp = False

for kp_name in kp_variants:
    print(f"\nПопытка: {kp_name}")
    
    try:
        # Простой запрос без фильтра
        endpoint = urljoin(BASE_URL, f"{kp_name}?$top=5")
        response = requests.get(endpoint, auth=auth, timeout=10, verify=False)
        
        if response.status_code == 200:
            print(f"  ✓ Найден! Статус {response.status_code}")
            found_kp = True
            
            try:
                data = response.json()
                
                if 'value' in data and len(data['value']) > 0:
                    print(f"  ✓ Получено {len(data['value'])} документов")
                    
                    # Выводим структуру первого документа
                    first_doc = data['value'][0]
                    print(f"\n  Структура документа (поля в первом документе):")
                    for key in sorted(first_doc.keys()):
                        value = first_doc[key]
                        if isinstance(value, str) and len(str(value)) > 100:
                            value = str(value)[:100] + "..."
                        print(f"    - {key}: {value}")
                    
                    print(f"\n  Все документы:")
                    for doc in data['value']:
                        print(f"    {json.dumps(doc, indent=6, ensure_ascii=False)}")
                else:
                    print(f"  ℹ Объект найден, но документов нет или не распарсился JSON")
                    print(f"  Ответ: {response.text[:300]}")
                    
            except json.JSONDecodeError as je:
                print(f"  ℹ Ошибка при парсинге JSON: {je}")
                print(f"  Ответ: {response.text[:300]}")
                
        elif response.status_code == 404:
            print(f"  ✗ Объект не найден (404)")
        else:
            print(f"  ✗ Статус {response.status_code}")
            
    except Exception as e:
        print(f"  ✗ Ошибка: {e}")

if not found_kp:
    print("\n⚠ Коммерческие предложения не найдены ни под одним названием")
    print("Возможно, нужно использовать другие имена объектов")

# === ТЕСТ 4: Фильтр по датам (если КП найдены) ===
if found_kp:
    print("\n\n4. ПОИСК КП ЗА МАРТ 2026")
    print("-" * 80)
    
    try:
        # Формируем OData фильтр для даты
        # Формат OData: DateTime'2026-03-01T00:00:00'
        march_start = "2026-03-01T00:00:00"
        march_end = "2026-03-31T23:59:59"
        
        # Попробуем несколько вариантов фильтра
        filters = [
            f"$filter=Date ge datetime'{march_start}'",
            f"$filter=CreatedDate ge datetime'{march_start}'",
            f"$filter=Дата ge datetime'{march_start}'",
        ]
        
        for filter_str in filters:
            print(f"\nПопытка: {filter_str}")
            
            try:
                endpoint = urljoin(BASE_URL, f"Document_КоммерческоеПредложениеКлиенту?{filter_str}&$top=10")
                response = requests.get(endpoint, auth=auth, timeout=10, verify=False)
                
                if response.status_code == 200:
                    data = response.json()
                    count = len(data.get('value', []))
                    print(f"  ✓ Фильтр работает! Найдено {count} документов")
                    
                    if count > 0:
                        print("\n  Примеры документов:")
                        for doc in data['value'][:3]:
                            print(f"    {json.dumps(doc, indent=8, ensure_ascii=False)}")
                        break
                else:
                    print(f"  ✗ Статус {response.status_code}")
                    
            except Exception as e:
                print(f"  ✗ Ошибка: {e}")
                
    except Exception as e:
        print(f"Ошибка при попытке фильтрации: {e}")

print("\n" + "=" * 80)
print("ИССЛЕДОВАНИЕ ЗАВЕРШЕНО")
print("=" * 80)
print("\nСледующие шаги:")
print("1. Проверить какие объекты найдены в метаданных")
print("2. Если КП найдены, определить правильные имена полей")
print("3. Уточнить фильтры для поиска по датам и статусу")
print("=" * 80)
