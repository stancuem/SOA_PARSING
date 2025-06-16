import os
import requests
import logging
from tqdm import tqdm
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("downloader")

# Базовый URL для OpenAlex S3
BASE_URL = "https://openalex.s3.amazonaws.com/data/"

# Список валидных дат для всех сущностей
VALID_DATES = [
    "updated_date=2025-05-15",
    "updated_date=2025-05-16",
    "updated_date=2025-05-17",
    "updated_date=2025-05-18",
    "updated_date=2025-05-19",
    "updated_date=2025-05-20",
    "updated_date=2025-05-21",
    "updated_date=2025-05-22",
    "updated_date=2025-05-23",
    "updated_date=2025-05-24"
]

# Подмножества данных для загрузки (сущность: максимальное количество файлов для проверки)
ENTITIES = {
    "works": 1,      # Только part_000.gz
    "authors": 5,    # Проверяем до part_004.gz
    "concepts": 1,   # Только part_000.gz
    "institutions": 1, # Только part_000.gz
    "sources": 1,    # Только part_000.gz (бывшие venues)
    "publishers": 1  # Только part_000.gz
}

# Создаем директории для хранения данных
def create_directories():
    os.makedirs("data", exist_ok=True)
    # Создаем отдельную директорию для каждой сущности
    for entity in ENTITIES.keys():
        os.makedirs(f"data/{entity}", exist_ok=True)
    logger.info("Директории для данных созданы")

# Проверка существования файла на сервере
def check_file_exists(url):
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except Exception:
        return False

# Загрузка файла с повторными попытками
def download_file(url, output_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Получаем размер файла
            total_size = int(response.headers.get('content-length', 0))
            
            # Загружаем файл с индикатором прогресса
            with open(output_path, 'wb') as f:
                with tqdm(
                    total=total_size, 
                    unit='B', 
                    unit_scale=True, 
                    desc=os.path.basename(output_path)
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            # Проверяем размер загруженного файла
            if os.path.getsize(output_path) > 0:
                return True
            else:
                logger.warning(f"Загруженный файл пуст: {output_path}, попытка {attempt+1}/{max_retries}")
                time.sleep(2)
        except Exception as e:
            logger.error(f"Ошибка при загрузке {url}: {str(e)}, попытка {attempt+1}/{max_retries}")
            time.sleep(2)
    
    logger.error(f"Не удалось загрузить {url} после {max_retries} попыток")
    return False

# Основная функция загрузки данных
def download_data():
    create_directories()
    
    downloaded_files = 0
    failed_files = 0
    
    logger.info(f"Начало загрузки данных из OpenAlex S3")
    
    # Загружаем данные для каждой сущности
    for entity, max_parts in ENTITIES.items():
        logger.info(f"Обработка сущности '{entity}'")
        entity_downloaded = 0
        
        # Для каждой сущности проверяем все даты
        for date in VALID_DATES:
            # Для каждой даты проверяем наличие файлов part_000.gz, part_001.gz и т.д.
            for part_num in range(max_parts):
                # Для works используем часть пути из date
                if entity == "works":
                    filename = f"{entity}/{date}/part_{part_num:03d}.gz"
                    output_filename = f"updated_date_{date.split('=')[1]}_part_{part_num:03d}.jsonl.gz"
                else:
                    # Для других сущностей используем полный путь с датой
                    filename = f"{entity}/{date}/part_{part_num:03d}.gz"
                    output_filename = f"{date.split('=')[1]}_part_{part_num:03d}.jsonl.gz"
                
                url = BASE_URL + filename
                output_path = f"data/{entity}/{output_filename}"
                
                # Проверяем существование файла
                if check_file_exists(url):
                    logger.info(f"Найден файл {url}")
                    logger.info(f"Загрузка {url} -> {output_path}")
                    
                    success = download_file(url, output_path)
                    if success:
                        downloaded_files += 1
                        entity_downloaded += 1
                        logger.info(f"Успешно загружен файл {entity}/{date}/part_{part_num:03d}.gz")
                    else:
                        failed_files += 1
                else:
                    logger.info(f"Файл не существует: {url}")
        
        logger.info(f"Загружено {entity_downloaded} файлов для сущности '{entity}'")
    
    logger.info(f"Загрузка завершена. Успешно: {downloaded_files}, Ошибок: {failed_files}")
    return downloaded_files, failed_files

if __name__ == "__main__":
    download_data() 