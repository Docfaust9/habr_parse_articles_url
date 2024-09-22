import csv
import time
import logging
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

# Загружаем настройки из .env файла
load_dotenv()
FIREFOX_PATH = os.getenv('FIREFOX_PATH')
GECKO_DRIVER_PATH = os.getenv('GECKO_DRIVER_PATH')
ADBLOCK_EXTENSION_PATH = os.getenv('ADBLOCK_EXTENSION_PATH')
CPU_THREADS = os.getenv('CPU_THREADS')

# Создаем директории для CSV-файлов и логов
PARSED_DIR = Path('parsed')
LOGS_DIR = Path('logs')
PARSED_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Настройки логгирования
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
LOG_DATEFMT = '%Y-%m-%d_%H-%M-%S'
LOG_FILENAME = LOGS_DIR / f'script_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt=LOG_DATEFMT,
    filename=LOG_FILENAME,
    filemode='w'
)

# Создаем обработчик для вывода логов в терминал
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATEFMT))
logging.getLogger().addHandler(console_handler)

# Параметры парсинга
NUM_PAGES_PER_SEARCH = 16  # Количество страниц для каждого хаба
ALLOWED_DIFFICULTIES = ["/hard", "/medium"]  # Разрешенные уровни сложности
SEARCH_TAGS = [
    "https://habr.com/ru/hubs/infosecurity/articles/top/alltime{difficulty}/page{page_number}/",
    "https://habr.com/ru/hubs/sys_admin/articles/top/alltime{difficulty}/page{page_number}/",
    "https://habr.com/ru/hubs/business-laws/articles/top/alltime{difficulty}/page{page_number}/",
    "https://habr.com/ru/hubs/linux/articles/top/alltime{difficulty}/page{page_number}/",
    "https://habr.com/ru/hubs/devops/articles/top/alltime{difficulty}/page{page_number}/",
    "https://habr.com/ru/hubs/s_admin/articles/top/alltime{difficulty}/page{page_number}/",
     "https://habr.com/ru/hubs/itstandarts/articles/top/alltime{difficulty}/page{page_number}/"]

ARTICLE_PARSE_TIMEOUT = 4
SCROLL_PAUSE_TIME = 2

# Функция инициализации драйвера
def initialize_driver():
    """Инициализация драйвера Firefox с расширением Adblock Plus"""
    options = Options()
    options.binary_location = FIREFOX_PATH
    options.add_argument("--headless")
    options.page_load_strategy = 'eager'
    options.accept_insecure_certs = True
    service = Service(executable_path=GECKO_DRIVER_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    driver.install_addon(ADBLOCK_EXTENSION_PATH, temporary=False)
    return driver

# Функция открытия страницы хаба
def open_habr_hub(driver, hub_url, page_number, difficulty=''):
    """Открытие страницы хаба на Habr с проверкой, существует ли страница"""
    url = hub_url.format(page_number=page_number, difficulty=difficulty)
    driver.get(url)
    logging.info("Открыта страница хаба: %s", url)
    
    try:
        # Проверяем наличие элементов, которые указывают на существование страницы
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'article.tm-articles-list__item'))
        )
        logging.info("Страница %s существует", url)
        return True
    except Exception as e:
        logging.warning("Страница %s не существует: %s", url, e)
        return False

# Функция для прокрутки страницы
def scroll_to_load_articles(driver):
    """Прокрутка страницы для подгрузки статей"""
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    logging.info("Прокрутка страницы завершена")

# Функция парсинга статей
def parse_article(article):
    """Парсинг заголовка и ссылки на статью"""
    title_elem = WebDriverWait(article, ARTICLE_PARSE_TIMEOUT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'a.tm-title__link[data-test-id="article-snippet-title-link"]'))
    )
    title = title_elem.text
    link = title_elem.get_attribute('href')
    return {'title': title, 'link': link}

# Парсинг статей на странице
def parse_articles(driver):
    """Парсинг списка статей"""
    articles = driver.find_elements(By.CSS_SELECTOR, 'article.tm-articles-list__item')
    parsed_articles = []

    for article in articles:
        try:
            parsed_article = parse_article(article)
            parsed_articles.append(parsed_article)
            logging.info("Parsed article: %s - %s", parsed_article['title'], parsed_article['link'])
        except Exception as e:
            logging.warning("Failed to parse article: %s", e)
            continue

    return parsed_articles

# Функция для сохранения данных в CSV
def save_to_csv(parsed_articles, filename):
    """Сохранение данных в CSV-файл"""
    unique_links = set(article['link'] for article in parsed_articles)

    with open(PARSED_DIR / filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Link'])
        for link in unique_links:
            article = next((a for a in parsed_articles if a['link'] == link), None)
            if article:
                writer.writerow([article['title'], article['link']])
    logging.info("Data saved to %s", filename)


# Функция для параллельного парсинга
def parse_hub(hub_url, difficulty):
    """Парсинг всех страниц хаба для заданной сложности"""
    driver = initialize_driver()
    parsed_articles = []

    for page_number in range(1, NUM_PAGES_PER_SEARCH + 1):
        if open_habr_hub(driver, hub_url, page_number, difficulty):
            scroll_to_load_articles(driver)
            parsed_articles.extend(parse_articles(driver))
        else:
            break  # Прекращаем загрузку, если страница не существует

    driver.quit()
    return parsed_articles


# Основная функция
def main():
    """Основная функция с параллельной обработкой хабов"""
    parsed_articles = []

    # Используем ThreadPoolExecutor для параллельного выполнения
    with ThreadPoolExecutor(max_workers=CPU_THREADS) as executor:
        futures = []
        for hub_url in SEARCH_TAGS:
            for difficulty in ALLOWED_DIFFICULTIES:
                futures.append(
                    executor.submit(parse_hub, hub_url, difficulty)
                )

        # Ожидание завершения всех задач
        for future in as_completed(futures):
            result = future.result()
            if result:
                parsed_articles.extend(result)

    # Сохранение данных в CSV
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f'articles_{now}.csv'
    save_to_csv(parsed_articles, filename)
    total_articles = len(parsed_articles)
    logging.info("Total number of articles parsed: %s", total_articles)

if __name__ == "__main__":
    main()
