import requests
from bs4 import BeautifulSoup
from lxml import etree
import re
import json
from pathlib import Path
from loguru import logger
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import xml.etree.ElementTree as ET
import os
from datetime import datetime, timedelta
import sys


# Конфигурация
SITE_URL = "https://i-sosna.ru"
FEED_URL = "https://i-sosna.ru/bitrix/catalog_export/min_ya_feed.xml"
COLLECTIONS_URL = f"{SITE_URL}/catalog/series/"
OUTPUT_FEED_PATH = "docs/yandex_feed.xml"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 15
MAX_WORKERS = 5


def setup_logging():
    """Настройка логирования"""
    logger.remove()
    logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
    logger.add("feed_processor.log", rotation="10 MB", retention="1 week", level="DEBUG")


def fetch_url(url, retries=MAX_RETRIES):
    """
    Получение содержимого URL с повторными попытками при ошибках
    
    Args:
        url: URL для загрузки
        retries: Количество попыток
    
    Returns:
        requests.Response или None при неудаче
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 XML_CREATOR_BOT'
    }
    
    for attempt in range(retries):
        try:
            sleep(0.5)
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                logger.warning(f"URL не найден (404): {url}")
                return None
            else:
                logger.warning(f"Попытка {attempt + 1}/{retries} неудачна для {url}. Код статуса: {response.status_code}")
                if attempt < retries - 1:
                    sleep(1)
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе {url}: {str(e)}")
            if attempt < retries - 1:
                sleep(1)
    
    logger.error(f"Не удалось загрузить URL после {retries} попыток: {url}")
    return None


def parse_existing_feed(feed_content):
    """
    Парсим существующий XML-фид
    
    Args:
        feed_content: Содержимое XML-файда
    
    Returns:
        root: Корневой элемент XML
    """
    try:
        parser = ET.XMLParser(encoding="utf-8")
        root = ET.fromstring(feed_content, parser=parser)
        logger.info("Фид успешно распарсен")
        return root
    except ET.ParseError as e:
        logger.error(f"Ошибка парсинга XML: {str(e)}")
        raise


def extract_prices_from_html(html_content):
    """
    Извлекаем цены с HTML-страницы товара
    
    Args:
        html_content: HTML-содержимое страницы
    
    Returns:
        tuple: (price, oldprice) или (None, None) если цены не найдены
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        price_box = soup.find('div', class_='price-box')
        
        if not price_box:
            logger.warning("Блок с ценами не найден на странице")
            return None, None
        
        # Ищем текущую цену
        current_price_element = price_box.find('div', class_='price')
        if current_price_element:
            # Берем текст из strong тега
            price_text = current_price_element.find('strong').get_text(strip=True)
            # Убираем пробелы и символы
            price_clean = re.sub(r'[^\d]', '', price_text)
            if price_clean:
                price = int(price_clean)
        
        # Ищем старую цену (если есть скидка)
        old_price_element = price_box.find('div', class_='old-price')
        if old_price_element:
            old_price_text = old_price_element.find('strong').get_text(strip=True)
            old_price_clean = re.sub(r'[^\d]', '', old_price_text)
            if old_price_clean:
                old_price = int(old_price_clean)
        else:
            old_price = None
        
        if 'price' in locals():
            logger.debug(f"Найдены цены - текущая: {price}, старая: {old_price if old_price else 'нет'}")
            return price, old_price if old_price and old_price > price else None
        else:
            logger.warning("Текущая цена не найдена на странице")
            return None, None
            
    except Exception as e:
        logger.error(f"Ошибка при извлечении цен: {str(e)}")
        return None, None


def process_offer_price(offer_element):
    """
    Обрабатываем один offer для обновления цены
    
    Args:
        offer_element: XML-элемент offer
    
    Returns:
        tuple: (offer_id, price, oldprice) или (offer_id, None, None) если ошибка
    """
    offer_id = offer_element.get('id')
    url_element = offer_element.find('url')
    
    if url_element is None or not url_element.text:
        logger.warning(f"У товара с ID {offer_id} отсутствует URL")
        return offer_id, None, None
    
    url = url_element.text.strip()
    logger.debug(f"Обработка цен для товара ID {offer_id}, URL: {url}")
    
    response = fetch_url(url)
    if not response:
        return offer_id, None, None
    
    price, oldprice = extract_prices_from_html(response.text)
    return offer_id, price, oldprice


def update_prices_in_feed(root, test_mode=False, test_limit=1000):
    """
    Обновляем цены во всем фиде
    
    Args:
        root: Корневой XML-элемент
    
    Returns:
        list: Список ID товаров, которые нужно удалить
    """

    offers = root.findall(".//offer")
    
    # Если в тестовом режиме, ограничиваем количество offer
    if test_mode:
        logger.info(f"Тестовый режим: обработка только {test_limit} товаров из {len(offers)}")
        offers = offers[:test_limit]
        
    logger.info(f"Найдено товаров для обновления цен: {len(offers)}")
    
    offers_to_remove = []
    
    # Используем ThreadPoolExecutor для параллельной обработки
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_offer = {
            executor.submit(process_offer_price, offer): offer 
            for offer in offers
        }
        
        for future in as_completed(future_to_offer):
            offer_element = future_to_offer[future]
            try:
                offer_id, price, oldprice = future.result()
                
                if price is None:
                    logger.warning(f"Не удалось обновить цену для товара ID {offer_id}")
                    offers_to_remove.append(offer_id)
                else:
                    # Обновляем текущую цену
                    price_element = offer_element.find('price')
                    if price_element is not None:
                        old_price_value = price_element.text
                        price_element.text = str(price)
                        logger.info(f"Товар ID {offer_id}: цена обновлена с {old_price_value} на {price}")
                    else:
                        logger.warning(f"Элемент <price> не найден для товара ID {offer_id}")
                    
                    # Обновляем или добавляем oldprice
                    oldprice_element = offer_element.find('oldprice')
                    if oldprice:
                        if oldprice_element is not None:
                            oldprice_element.text = str(oldprice)
                            logger.info(f"Товар ID {offer_id}: oldprice установлен в {oldprice}")
                        else:
                            new_oldprice = ET.SubElement(offer_element, 'oldprice')
                            new_oldprice.text = str(oldprice)
                            logger.info(f"Товар ID {offer_id}: добавлен oldprice {oldprice}")
                    elif oldprice_element is not None:
                        # Удаляем oldprice, если скидка отсутствует
                        offer_element.remove(oldprice_element)
                        logger.info(f"Товар ID {offer_id}: oldprice удален, скидка отсутствует")
                        
            except Exception as e:
                logger.error(f"Ошибка при обработке товара: {str(e)}")
                offer_id = offer_element.get('id')
                offers_to_remove.append(offer_id)
    
    return offers_to_remove


def parse_collections_from_html(html_content):
    """
    Парсим коллекции из HTML
    
    Args:
        html_content: HTML содержимое страницы с коллекциями
    
    Returns:
        dict: Словарь с информацией о коллекциях
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Ищем контейнер коллекций
        collections_container = soup.find('ul', class_='series-brand')
        
        if not collections_container:
            logger.error("Контейнер с коллекциями (.series-brand) не найден на странице")
            return {}
        
        collection_items = collections_container.find_all('li', class_='series-brand__item-col')
        
        if not collection_items:
            logger.warning("Не найдено элементов коллекций на странице")
            return {}
        
        logger.info(f"Найдено элементов коллекций: {len(collection_items)}")
        collections = {}
        
        for idx, item in enumerate(collection_items, 1):
            try:
                # Название коллекции
                name_elem = item.select_one('.series-brand__name')
                if not name_elem:
                    logger.debug(f"Элемент названия коллекции не найден для элемента #{idx}")
                    continue
                
                name = name_elem.get_text(strip=True)
                if not name:
                    logger.debug(f"Пустое название коллекции для элемента #{idx}")
                    continue
                
                # URL коллекции
                link_elem = item.select_one('a.series-brand__inner')
                if not link_elem or not link_elem.get('href'):
                    logger.debug(f"Ссылка коллекции не найдена для {name}")
                    continue
                
                url = urljoin(SITE_URL, link_elem['href'].strip())
                
                # Фоновое изображение
                style_attr = link_elem.get('style', '')
                image_url = None
                if style_attr:
                    # Расширенный поиск URL в стилях
                    match = re.search(r"url\(['\"]?([^'\")]+)['\"]?\)", style_attr)
                    if match:
                        image_path = match.group(1).strip()
                        if image_path.startswith(('http://', 'https://')):
                            image_url = image_path
                        elif image_path.startswith('/'):
                            image_url = urljoin(SITE_URL, image_path)
                        else:
                            logger.warning(f"Неизвестный формат пути изображения: {image_path}")
                
                # Производитель (описание)
                description = f"{name} в интернет-магазине Гармония дерева"
                description_elem = item.select_one('.series-brand__title')
                if description_elem:
                    manufacturer_text = description_elem.get_text(strip=True)
                    if manufacturer_text:
                        # Очищаем текст от лишних частей
                        manufacturer = manufacturer_text.replace('Производитель - ', '').strip()
                        if manufacturer:
                            description = f"{name} от производителя {manufacturer} в интернет-магазине Гармония дерева"
                
                collections[name] = {
                    'id': idx,
                    'url': url,
                    'picture': image_url,
                    'name': name,
                    'description': description
                }
                logger.debug(f"Добавлена коллекция: {name} (ID: {idx}, URL: {url})")
                
            except Exception as e:
                logger.error(f"Ошибка при обработке коллекции #{idx}: {str(e)}")
        
        logger.info(f"Успешно обработано коллекций: {len(collections)}")
        return collections
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге коллекций: {str(e)}")
        return {}


def create_collections_xml(collections_dict):
    """
    Создаем XML-структуру для коллекций
    
    Args:
        collections_dict: Словарь с данными о коллекциях
    
    Returns:
        ET.Element: XML-элемент collections
    """
    collections_elem = ET.Element('collections')
    
    for name, data in collections_dict.items():
        collection_elem = ET.SubElement(collections_elem, 'collection')
        collection_elem.set('id', str(data['id']))
        
        # URL
        url_elem = ET.SubElement(collection_elem, 'url')
        url_elem.text = data['url']
        
        # Изображение
        if data['picture']:
            picture_elem = ET.SubElement(collection_elem, 'picture')
            picture_elem.text = data['picture']
        
        # Название
        name_elem = ET.SubElement(collection_elem, 'name')
        name_elem.text = data['name']
        
        # Описание
        desc_elem = ET.SubElement(collection_elem, 'description')
        desc_elem.text = data['description']
    
    logger.info("XML для коллекций успешно создан")
    return collections_elem


def update_offers_with_collections(root, collections_dict, test_mode=False, test_limit=1000):
    """
    Обновляем offers, добавляя collectionId и удаляя параметр Коллекция
    
    Args:
        root: Корневой XML-элемент
        collections_dict: Словарь коллекций
    
    Returns:
        int: Количество обновленных товаров
    """
    offers = root.findall(".//offer")
    
    if test_mode:
        logger.info(f"Тестовый режим: обработка коллекций только для {test_limit} товаров")
        offers = offers[:test_limit]
        
    updated_count = 0
    
    for offer in offers:
        # Ищем параметр "Коллекция"
        collection_param = None
        for param in offer.findall('param'):
            if param.get('name') == 'Коллекция':
                collection_param = param
                break
        
        if collection_param is not None and collection_param.text:
            collection_name = collection_param.text.strip()
            if collection_name in collections_dict:
                # Добавляем collectionId
                collection_id = collections_dict[collection_name]['id']
                collection_id_elem = ET.SubElement(offer, 'collectionId')
                collection_id_elem.text = str(collection_id)
                logger.debug(f"Товар ID {offer.get('id')} отнесен к коллекции '{collection_name}' (ID: {collection_id})")
                
                # Удаляем параметр "Коллекция"
                offer.remove(collection_param)
                updated_count += 1
    
    logger.info(f"Обновлено товаров с коллекциями: {updated_count}")
    return updated_count


def remove_offers(root, offer_ids):
    """
    Удаляем offers по списку ID
    
    Args:
        root: Корневой XML-элемент
        offer_ids: Список ID для удаления
    """
    offers_element = root.find('offers')
    if offers_element is None:
        logger.warning("Элемент <offers> не найден в XML")
        return
    
    removed_count = 0
    for offer in list(offers_element):
        offer_id = offer.get('id')
        if offer_id in offer_ids:
            offers_element.remove(offer)
            removed_count += 1
            logger.info(f"Товар ID {offer_id} удален из фида")
    
    logger.info(f"Всего удалено товаров: {removed_count}")


def pretty_xml(element, level=0):
    """
    Форматируем XML для красивого вывода
    
    Args:
        element: XML-элемент
        level: Уровень вложенности
    """
    indent = "  " * level
    
    if len(element):
        if not element.text or not element.text.strip():
            element.text = "\n" + indent + "  "
        
        for child in element:
            pretty_xml(child, level + 1)
            
        if not child.tail or not child.tail.strip():
            child.tail = "\n" + indent
    
    if level and (not element.tail or not element.tail.strip()):
        element.tail = "\n" + indent


def save_feed(root):
    """
    Сохраняем обновленный фид в файл
    
    Args:
        root: Корневой XML-элемент
    """
    # Форматируем XML
    pretty_xml(root)
    
    # Создаем XML-строку
    xml_str = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
    
    # Добавляем XML декларацию
    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str
    
    # Сохраняем в файл
    with open(OUTPUT_FEED_PATH, 'w', encoding='utf-8') as f:
        f.write(xml_str)
    
    logger.info(f"Обновленный фид успешно сохранен в {OUTPUT_FEED_PATH}")


def main():
    """Основная функция программы"""
    test_mode = False
    test_limit = 1000
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_mode = True
        logger.info(f"🚀 ЗАПУЩЕН ТЕСТОВЫЙ РЕЖИМ: будет обработано только {test_limit} товаров")

    setup_logging()
    logger.info("=== Начало обработки фида ===")
    
    try:
        # Шаг 1: Загружаем существующий фид
        logger.info(f"Загрузка фида с URL: {FEED_URL}")
        response = fetch_url(FEED_URL)
        if not response:
            logger.error("Не удалось загрузить фид")
            return
        
        # Шаг 2: Парсим фид
        root = parse_existing_feed(response.content)
        
        # Шаг 3: Обновляем цены
        logger.info("=== Обновление цен ===")
        offers_to_remove = update_prices_in_feed(root, test_mode, test_limit)
        
        # Шаг 4: Загружаем страницу с коллекциями
        logger.info("=== Обработка коллекций ===")
        logger.info(f"Загрузка страницы коллекций: {COLLECTIONS_URL}")
        collections_response = fetch_url(COLLECTIONS_URL)
        if not collections_response:
            logger.error("Не удалось загрузить страницу коллекций")
        else:
            # Парсим коллекции
            collections_dict = parse_collections_from_html(collections_response.text)
            
            # Создаем XML-структуру для коллекций
            collections_xml = create_collections_xml(collections_dict)
            
            # Вставляем коллекции в фид
            shop_element = root.find('shop')
            if shop_element is not None:
                # Удаляем существующие коллекции, если они есть
                existing_collections = shop_element.find('collections')
                if existing_collections is not None:
                    shop_element.remove(existing_collections)
                
                # Находим элемент offers для правильного размещения
                offers_element = shop_element.find('offers')
                if offers_element is not None:
                    # Получаем индекс элемента offers
                    offers_index = list(shop_element).index(offers_element)
                    # Вставляем коллекции ПОСЛЕ offers
                    shop_element.insert(offers_index + 1, collections_xml)
                else:
                    # Если нет offers, добавляем в конец
                    shop_element.append(collections_xml)
                logger.info("Коллекции добавлены в фид после offers")
                
                # Обновляем offers коллекциями
                update_offers_with_collections(root, collections_dict, test_mode, test_limit)
        
        # Шаг 5: Удаляем товары, для которых не удалось обновить цены
        if offers_to_remove:
            logger.info(f"=== Удаление {len(offers_to_remove)} товаров с неактуальными ценами ===")
            remove_offers(root, offers_to_remove)
        
        # Шаг 6: Обновляем дату в фиде
        current_time = datetime.now().astimezone().isoformat(timespec='seconds')
        # Формат будет: 2025-11-18T19:24:50+03:00
        root.set('date', current_time)
        logger.info(f"Дата фида обновлена: {current_time}")
        
        if test_mode:
            logger.info(f"Тестовый режим: фильтрация товаров до {test_limit} шт.")
            offers_element = root.find('.//offers')
            if offers_element is not None:
                all_offers = list(offers_element)
                # Оставляем только первые test_limit товаров
                offers_to_keep = all_offers[:test_limit]
                offers_to_remove = all_offers[test_limit:]
                
                for offer in offers_to_remove:
                    offers_element.remove(offer)
                
                logger.info(f"В тестовом режиме оставлено товаров: {len(offers_to_keep)} из {len(all_offers)}")
                
        # Шаг 7: Сохраняем обновленный фид
        save_feed(root)
        
        logger.info("=== Обработка успешно завершена ===")
    
    except Exception as e:
        logger.exception(f"Критическая ошибка при обработке фида: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
