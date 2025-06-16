import os
import json
import gzip
import logging
import pandas as pd
from tqdm import tqdm
import time
from collections import defaultdict

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
                    
                    # Фильтрация по году публикации и типу
                    publication_year = work.get('publication_year')
                    work_type = work.get('type')
                    
                    if (publication_year is not None and publication_year >= 2020 and 
                        (work_type == "journal-article" or work_type is None)):
                        
                        work_id = work.get('id')
                        if not work_id:
                            continue
                        
                        # Извлечение данных о публикации
                        work_data = {
                            'id': work_id,
                            'title': work.get('title', ''),
                            'publication_year': publication_year,
                            'doi': work.get('doi', ''),
                            'cited_by_count': work.get('cited_by_count', 0),
                            'type': work_type
                        }
                        
                        # Добавление данных о публикации
                        works_data.append(work_data)
                        
                        # Обработка source (источника)
                        host_venue = work.get('host_venue', {})
                        source_id = host_venue.get('id')
                        if source_id:
                            source_ids.add(source_id)
                            work_source_relations.append({
                                'work_id': work_id,
                                'source_id': source_id
                            })
                            
                            # Извлечение издателя
                            publisher = host_venue.get('publisher')
                            if publisher:
                                publisher_names.add(publisher)
                        
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