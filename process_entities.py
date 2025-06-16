import os
import json
import gzip
import logging
import pandas as pd
from tqdm import tqdm
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("entity_processor")

# Директории для данных
DATA_DIR = "data"
OUTPUT_DIR = "output"

# Загрузка множеств ID связанных сущностей
def load_entity_ids():
    try:
        with open(os.path.join(OUTPUT_DIR, "entity_ids.json"), "r") as f:
            entity_ids = json.load(f)
            
            # Преобразование списков обратно в множества
            for key in entity_ids:
                entity_ids[key] = set(entity_ids[key])
                
            logger.info(f"Загружены ID связанных сущностей")
            return entity_ids
    except Exception as e:
        logger.error(f"Ошибка при загрузке ID сущностей: {str(e)}")
        return None

# Обработка авторов
def process_authors(author_ids):
    authors_data = []
    author_institution_relations = []
    
    authors_dir = os.path.join(DATA_DIR, "authors")
    
    # Проверяем, существует ли директория authors
    if not os.path.exists(authors_dir):
        logger.error(f"Директория {authors_dir} не найдена. Убедитесь, что данные были загружены.")
        return
    
    author_files = sorted([f for f in os.listdir(authors_dir) if f.startswith("part_") and f.endswith(".jsonl.gz")])
    
    logger.info(f"Начало обработки авторов")
    
    for author_file in author_files:
        file_path = os.path.join(authors_dir, author_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {author_file}"):
                try:
                    author = json.loads(line)
                    author_id = author.get('id')
                    
                    if author_id in author_ids:
                        # Извлечение данных об авторе
                        author_data = {
                            'id': author_id,
                            'name': author.get('display_name', ''),
                            'orcid': author.get('orcid', ''),
                            'works_count': author.get('works_count', 0),
                            'cited_by_count': author.get('cited_by_count', 0)
                        }
                        
                        authors_data.append(author_data)
                        
                        # Обработка связи с организацией
                        last_institution = author.get('last_known_institution', {})
                        institution_id = last_institution.get('id')
                        if institution_id:
                            author_institution_relations.append({
                                'author_id': author_id,
                                'institution_id': institution_id
                            })
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке автора: {str(e)}")
                    continue
    
    # Сохранение данных об авторах
    authors_df = pd.DataFrame(authors_data)
    authors_df.to_csv(os.path.join(OUTPUT_DIR, "authors.csv"), index=False)
    logger.info(f"Сохранено {len(authors_df)} авторов в authors.csv")
    
    # Сохранение связей автор-организация
    pd.DataFrame(author_institution_relations).to_csv(os.path.join(OUTPUT_DIR, "author_institution.csv"), index=False)
    logger.info(f"Сохранено {len(author_institution_relations)} связей автор-организация")

# Обработка организаций
def process_institutions(institution_ids):
    institutions_data = []
    
    institutions_dir = os.path.join(DATA_DIR, "institutions")
    
    # Проверяем, существует ли директория institutions
    if not os.path.exists(institutions_dir):
        logger.error(f"Директория {institutions_dir} не найдена. Убедитесь, что данные были загружены.")
        return
    
    institution_files = sorted([f for f in os.listdir(institutions_dir) if f.startswith("part_") and f.endswith(".jsonl.gz")])
    
    logger.info(f"Начало обработки организаций")
    
    for institution_file in institution_files:
        file_path = os.path.join(institutions_dir, institution_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {institution_file}"):
                try:
                    institution = json.loads(line)
                    institution_id = institution.get('id')
                    
                    if institution_id in institution_ids:
                        # Извлечение данных об организации
                        institution_data = {
                            'id': institution_id,
                            'display_name': institution.get('display_name', ''),
                            'country_code': institution.get('country_code', ''),
                            'type': institution.get('type', ''),
                            'works_count': institution.get('works_count', 0),
                            'cited_by_count': institution.get('cited_by_count', 0)
                        }
                        
                        institutions_data.append(institution_data)
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке организации: {str(e)}")
                    continue
    
    # Сохранение данных об организациях
    institutions_df = pd.DataFrame(institutions_data)
    institutions_df.to_csv(os.path.join(OUTPUT_DIR, "institutions.csv"), index=False)
    logger.info(f"Сохранено {len(institutions_df)} организаций в institutions.csv")

# Обработка концепций
def process_concepts(concept_ids):
    concepts_data = []
    concept_ancestor_relations = []
    
    concepts_dir = os.path.join(DATA_DIR, "concepts")
    
    # Проверяем, существует ли директория concepts
    if not os.path.exists(concepts_dir):
        logger.error(f"Директория {concepts_dir} не найдена. Убедитесь, что данные были загружены.")
        return
    
    concept_files = sorted([f for f in os.listdir(concepts_dir) if f.startswith("part_") and f.endswith(".jsonl.gz")])
    
    logger.info(f"Начало обработки концепций")
    
    for concept_file in concept_files:
        file_path = os.path.join(concepts_dir, concept_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {concept_file}"):
                try:
                    concept = json.loads(line)
                    concept_id = concept.get('id')
                    
                    if concept_id in concept_ids:
                        # Извлечение данных о концепции
                        concept_data = {
                            'id': concept_id,
                            'display_name': concept.get('display_name', ''),
                            'level': concept.get('level', 0),
                            'works_count': concept.get('works_count', 0),
                            'cited_by_count': concept.get('cited_by_count', 0)
                        }
                        
                        concepts_data.append(concept_data)
                        
                        # Обработка связей с предками
                        ancestors = concept.get('ancestors', [])
                        for ancestor in ancestors:
                            ancestor_id = ancestor.get('id')
                            if ancestor_id:
                                concept_ids.add(ancestor_id)  # Добавляем предков в множество концепций
                                concept_ancestor_relations.append({
                                    'concept_id': concept_id,
                                    'ancestor_id': ancestor_id
                                })
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке концепции: {str(e)}")
                    continue
    
    # Сохранение данных о концепциях
    concepts_df = pd.DataFrame(concepts_data)
    concepts_df.to_csv(os.path.join(OUTPUT_DIR, "concepts.csv"), index=False)
    logger.info(f"Сохранено {len(concepts_df)} концепций в concepts.csv")
    
    # Сохранение связей концепция-предок
    pd.DataFrame(concept_ancestor_relations).to_csv(os.path.join(OUTPUT_DIR, "concept_ancestor.csv"), index=False)
    logger.info(f"Сохранено {len(concept_ancestor_relations)} связей концепция-предок")

# Обработка источников (sources)
def process_sources(source_ids, publisher_names):
    sources_data = []
    source_publisher_relations = []
    
    sources_dir = os.path.join(DATA_DIR, "sources")
    
    # Проверяем, существует ли директория sources
    if not os.path.exists(sources_dir):
        logger.error(f"Директория {sources_dir} не найдена. Убедитесь, что данные были загружены.")
        return
    
    source_files = sorted([f for f in os.listdir(sources_dir) if f.startswith("part_") and f.endswith(".jsonl.gz")])
    
    logger.info(f"Начало обработки источников (sources)")
    
    for source_file in source_files:
        file_path = os.path.join(sources_dir, source_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {source_file}"):
                try:
                    source = json.loads(line)
                    source_id = source.get('id')
                    
                    if source_id in source_ids:
                        # Извлечение данных об источнике
                        source_data = {
                            'id': source_id,
                            'display_name': source.get('display_name', ''),
                            'issn': source.get('issn_l', ''),
                            'works_count': source.get('works_count', 0),
                            'cited_by_count': source.get('cited_by_count', 0)
                        }
                        
                        sources_data.append(source_data)
                        
                        # Обработка связи с издателем
                        publisher = source.get('publisher')
                        if publisher and publisher in publisher_names:
                            source_publisher_relations.append({
                                'source_id': source_id,
                                'publisher_name': publisher
                            })
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке источника: {str(e)}")
                    continue
    
    # Сохранение данных об источниках
    sources_df = pd.DataFrame(sources_data)
    sources_df.to_csv(os.path.join(OUTPUT_DIR, "sources.csv"), index=False)
    logger.info(f"Сохранено {len(sources_df)} источников в sources.csv")
    
    # Сохранение связей источник-издатель
    pd.DataFrame(source_publisher_relations).to_csv(os.path.join(OUTPUT_DIR, "source_publisher.csv"), index=False)
    logger.info(f"Сохранено {len(source_publisher_relations)} связей источник-издатель")

# Обработка издателей
def process_publishers(publisher_names):
    publishers_data = []
    
    publishers_dir = os.path.join(DATA_DIR, "publishers")
    
    # Проверяем, существует ли директория publishers
    if not os.path.exists(publishers_dir):
        logger.error(f"Директория {publishers_dir} не найдена. Убедитесь, что данные были загружены.")
        return
    
    publisher_files = sorted([f for f in os.listdir(publishers_dir) if f.startswith("part_") and f.endswith(".jsonl.gz")])
    
    logger.info(f"Начало обработки издателей")
    
    for publisher_file in publisher_files:
        file_path = os.path.join(publishers_dir, publisher_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {publisher_file}"):
                try:
                    publisher = json.loads(line)
                    publisher_name = publisher.get('display_name')
                    
                    if publisher_name in publisher_names:
                        # Извлечение данных об издателе
                        publisher_data = {
                            'name': publisher_name,
                            'works_count': publisher.get('works_count', 0),
                            'cited_by_count': publisher.get('cited_by_count', 0),
                            'country_codes': ','.join(publisher.get('country_codes', []))
                        }
                        
                        publishers_data.append(publisher_data)
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке издателя: {str(e)}")
                    continue
    
    # Сохранение данных об издателях
    publishers_df = pd.DataFrame(publishers_data)
    publishers_df.to_csv(os.path.join(OUTPUT_DIR, "publishers.csv"), index=False)
    logger.info(f"Сохранено {len(publishers_df)} издателей в publishers.csv")

# Основная функция обработки связанных сущностей
def process_entities():
    start_time = time.time()
    
    # Загрузка множеств ID связанных сущностей
    entity_ids = load_entity_ids()
    if not entity_ids:
        logger.error("Не удалось загрузить ID связанных сущностей. Убедитесь, что выполнен скрипт process_works.py")
        return
    
    # Обработка авторов
    process_authors(entity_ids["author_ids"])
    
    # Обработка организаций
    process_institutions(entity_ids["institution_ids"])
    
    # Обработка концепций
    process_concepts(entity_ids["concept_ids"])
    
    # Обработка источников
    process_sources(entity_ids["source_ids"], entity_ids["publisher_names"])
    
    # Обработка издателей
    process_publishers(entity_ids["publisher_names"])
    
    # Статистика
    end_time = time.time()
    processing_time = end_time - start_time
    
    logger.info(f"Обработка связанных сущностей завершена")
    logger.info(f"Время обработки: {processing_time:.2f} секунд")

if __name__ == "__main__":
    process_entities() 