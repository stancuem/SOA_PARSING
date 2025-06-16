import os
import json
import gzip
import logging
import pandas as pd
from tqdm import tqdm
import time
from collections import defaultdict, Counter

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("processor")

# Директории для данных
DATA_DIR = "data"
OUTPUT_DIR = "output"

# Максимальное количество публикаций для обработки
MAX_WORKS = 100_000

# Создание директории для выходных файлов
def create_output_directory():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"Создана директория для выходных файлов: {OUTPUT_DIR}")

# Функция для обработки публикаций (works)
def process_works():
    create_output_directory()
    
    # Множества для хранения ID связанных сущностей
    author_ids = set()
    concept_ids = set()
    institution_ids = set()
    source_ids = set()
    publisher_names = set()
    
    # Словари для хранения связей
    author_work_relations = []
    work_concept_relations = []
    work_source_relations = []
    work_citation_relations = []
    
    # Список для хранения данных о публикациях
    works_data = []
    
    # Счетчики
    processed_works = 0
    filtered_works = 0
    start_time = time.time()
    
    # Счетчик типов публикаций
    type_counter = Counter()
    
    # Обработка файлов works
    works_dir = os.path.join(DATA_DIR, "works")
    
    # Проверяем, существует ли директория works
    if not os.path.exists(works_dir):
        logger.error(f"Директория {works_dir} не найдена. Убедитесь, что данные были загружены.")
        return None
    
    # Ищем файлы с паттерном updated_date_YYYY-MM-DD.jsonl.gz
    work_files = sorted([f for f in os.listdir(works_dir) if f.startswith("updated_date_") and f.endswith(".jsonl.gz")])
    
    if not work_files:
        # Если файлы с новым паттерном не найдены, попробуем старый паттерн
        work_files = sorted([f for f in os.listdir(works_dir) if f.startswith("part_") and f.endswith(".jsonl.gz")])
    
    logger.info(f"Начало обработки публикаций (works)")
    logger.info(f"Найдено {len(work_files)} файлов works для обработки: {', '.join(work_files)}")
    
    for work_file in work_files:
        file_path = os.path.join(works_dir, work_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {work_file}"):
                try:
                    work = json.loads(line)
                    
                    # Удаляем фильтрацию по году публикации и типу
                    # Просто берем все публикации
                    
                    work_id = work.get('id')
                    if not work_id:
                        continue
                    
                    # Увеличиваем счетчик типа публикации
                    type_counter[work.get('type')] += 1
                    
                    # Извлечение данных о публикации
                    work_data = {
                        'id': work_id,
                        'title': work.get('title', ''),
                        'publication_year': work.get('publication_year'),
                        'doi': work.get('doi', ''),
                        'cited_by_count': work.get('cited_by_count', 0),
                        'type': work.get('type')
                    }
                    
                    # Добавление данных о публикации
                    works_data.append(work_data)
                    
                    # Обработка source (источника)
                    # Проверяем оба возможных места для source_id
                    host_venue = work.get('host_venue', {})
                    primary_location = work.get('primary_location', {})
                    
                    # Получаем source_id из host_venue или primary_location.source
                    source_id = None
                    publisher = None
                    
                    # Проверяем host_venue
                    if host_venue and isinstance(host_venue, dict):
                        source_id = host_venue.get('id')
                        publisher = host_venue.get('publisher')
                    
                    # Если не нашли в host_venue, проверяем primary_location.source
                    if not source_id and primary_location and isinstance(primary_location, dict):
                        source = primary_location.get('source', {})
                        if source and isinstance(source, dict):
                            source_id = source.get('id')
                            publisher = source.get('publisher')
                    
                    # Если нашли source_id, добавляем связь
                    if source_id:
                        source_ids.add(source_id)
                        work_source_relations.append({
                            'work_id': work_id,
                            'source_id': source_id
                        })
                        
                        # Добавляем издателя, если он есть
                        if publisher:
                            publisher_names.add(publisher)
                    
                    # Отладочная информация для первых 10 публикаций
                    if filtered_works < 10:
                        logger.info(f"Публикация {filtered_works+1}, ID: {work_id}")
                        logger.info(f"host_venue: {host_venue}")
                    
                    # Подсчет структуры host_venue
                    if filtered_works % 1000 == 0:
                        if host_venue is None:
                            logger.info(f"host_venue is None для публикации {work_id}")
                        elif not isinstance(host_venue, dict):
                            logger.info(f"host_venue не является словарем для публикации {work_id}, тип: {type(host_venue)}")
                        elif not host_venue:
                            logger.info(f"host_venue - пустой словарь для публикации {work_id}")
                    
                    # Обработка авторов
                    authorships = work.get('authorships', [])
                    for authorship in authorships:
                        author = authorship.get('author', {})
                        author_id = author.get('id')
                        if author_id:
                            author_ids.add(author_id)
                            author_work_relations.append({
                                'author_id': author_id,
                                'work_id': work_id
                            })
                            
                            # Извлечение организаций
                            institutions = authorship.get('institutions', [])
                            for institution in institutions:
                                institution_id = institution.get('id')
                                if institution_id:
                                    institution_ids.add(institution_id)
                    
                    # Обработка концепций
                    concepts = work.get('concepts', [])
                    for concept in concepts:
                        concept_id = concept.get('id')
                        if concept_id:
                            concept_ids.add(concept_id)
                            work_concept_relations.append({
                                'work_id': work_id,
                                'concept_id': concept_id,
                                'score': concept.get('score', 0)
                            })
                    
                    # Обработка цитирований
                    referenced_works = work.get('referenced_works', [])
                    for cited_id in referenced_works:
                        if cited_id:
                            work_citation_relations.append({
                                'citing_id': work_id,
                                'cited_id': cited_id
                            })
                    
                    filtered_works += 1
                    
                    # Проверка достижения лимита
                    if filtered_works >= MAX_WORKS:
                        logger.info(f"Достигнут лимит публикаций: {MAX_WORKS}")
                        break
                    
                    processed_works += 1
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке записи: {str(e)}")
                    continue
                
        if filtered_works >= MAX_WORKS:
            break
    
    # Сохранение данных о публикациях
    works_df = pd.DataFrame(works_data)
    works_df.to_csv(os.path.join(OUTPUT_DIR, "works.csv"), index=False)
    logger.info(f"Сохранено {len(works_df)} публикаций в works.csv")
    
    # Сохранение связей
    pd.DataFrame(author_work_relations).to_csv(os.path.join(OUTPUT_DIR, "author_work.csv"), index=False)
    logger.info(f"Сохранено {len(author_work_relations)} связей автор-публикация")
    
    pd.DataFrame(work_concept_relations).to_csv(os.path.join(OUTPUT_DIR, "work_concept.csv"), index=False)
    logger.info(f"Сохранено {len(work_concept_relations)} связей публикация-концепция")
    
    pd.DataFrame(work_source_relations).to_csv(os.path.join(OUTPUT_DIR, "work_source.csv"), index=False)
    logger.info(f"Сохранено {len(work_source_relations)} связей публикация-источник")
    
    pd.DataFrame(work_citation_relations).to_csv(os.path.join(OUTPUT_DIR, "work_citation.csv"), index=False)
    logger.info(f"Сохранено {len(work_citation_relations)} связей цитирования")
    
    # Сохранение множеств ID для последующей обработки
    with open(os.path.join(OUTPUT_DIR, "entity_ids.json"), "w") as f:
        json.dump({
            "author_ids": list(author_ids),
            "concept_ids": list(concept_ids),
            "institution_ids": list(institution_ids),
            "source_ids": list(source_ids),
            "publisher_names": list(publisher_names)
        }, f)
    
    # Статистика
    end_time = time.time()
    processing_time = end_time - start_time
    
    logger.info(f"Обработка публикаций завершена")
    logger.info(f"Всего обработано: {processed_works}, отфильтровано: {filtered_works}")
    logger.info(f"Типы публикаций: {dict(type_counter)}")
    logger.info(f"Время обработки: {processing_time:.2f} секунд")
    
    return {
        "author_ids": author_ids,
        "concept_ids": concept_ids,
        "institution_ids": institution_ids,
        "source_ids": source_ids,
        "publisher_names": publisher_names
    }

if __name__ == "__main__":
    process_works() 