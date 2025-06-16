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

# Ограничения для тестирования (количество сущностей каждого типа)
# Установите в None, чтобы обработать все сущности
MAX_AUTHORS = None          
MAX_INSTITUTIONS = None     
MAX_CONCEPTS = None         
MAX_SOURCES = None          
MAX_PUBLISHERS = None   

# Вспомогательная функция для нормализации ID
def normalize_id(id_value):
    """Обрезает префикс https://openalex.org/ у ID, если он присутствует."""
    if isinstance(id_value, str) and "openalex.org" in id_value:
        return id_value.split("/")[-1]
    return id_value

# Загрузка множеств ID связанных сущностей
def load_entity_ids():
    try:
        with open(os.path.join(OUTPUT_DIR, "entity_ids.json"), "r") as f:
            entity_ids = json.load(f)
            
            # Преобразование списков обратно в множества и обрезка префиксов URI
            for key in entity_ids:
                if key == "publisher_names":
                    # Для издателей просто преобразуем в множество (они не URI)
                    entity_ids[key] = set(entity_ids[key])
                else:
                    # Для остальных сущностей обрезаем префиксы URI
                    entity_ids[key] = {normalize_id(id) for id in entity_ids[key]}
                
            logger.info(f"Загружены ID связанных сущностей")
            logger.info(f"Количество ID: authors={len(entity_ids.get('author_ids', []))}, concepts={len(entity_ids.get('concept_ids', []))}, institutions={len(entity_ids.get('institution_ids', []))}, sources={len(entity_ids.get('source_ids', []))}, publishers={len(entity_ids.get('publisher_names', []))}")
            
            # Выводим примеры ID для проверки
            for key in entity_ids:
                if entity_ids[key]:
                    sample = list(entity_ids[key])[:3]  # Берем до 3 примеров
                    logger.info(f"Примеры {key}: {sample}")
            
            return entity_ids
    except Exception as e:
        logger.error(f"Ошибка при загрузке ID сущностей: {str(e)}")
        return None

# Обработка авторов
def process_authors(author_ids, entity_ids):
    authors_data = []
    author_institution_relations = []
    
    authors_dir = os.path.join(DATA_DIR, "authors")
    
    # Проверяем, существует ли директория authors
    if not os.path.exists(authors_dir):
        logger.error(f"Директория {authors_dir} не найдена. Убедитесь, что данные были загружены.")
        return
    
    # Ищем файлы с шаблоном YYYY-MM-DD_part_*.jsonl.gz
    author_files = sorted([f for f in os.listdir(authors_dir) if f.endswith(".jsonl.gz") and "_part_" in f])
    
    logger.info(f"Начало обработки авторов")
    logger.info(f"Найдено {len(author_files)} файлов авторов")
    
    # Счетчики для отладки
    total_authors = 0
    matched_authors = 0
    authors_with_institutions = 0
    
    # Загружаем ID организаций для проверки связей
    institution_ids = entity_ids["institution_ids"] if "institution_ids" in entity_ids else set()
    
    # Выводим примеры ID из entity_ids для проверки
    logger.info(f"Примеры ID из author_ids: {list(author_ids)[:5] if author_ids else []}")
    logger.info(f"Примеры ID из institution_ids: {list(institution_ids)[:5] if institution_ids else []}")
    
    for author_file in author_files:
        file_path = os.path.join(authors_dir, author_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {author_file}"):
                try:
                    author = json.loads(line)
                    raw_author_id = author.get('id')
                    author_id = normalize_id(raw_author_id)
                    
                    total_authors += 1
                    
                    if author_id in author_ids:
                        matched_authors += 1
                        
                        # Извлечение данных об авторе
                        author_data = {
                            'id': author_id,
                            'name': author.get('display_name', ''),
                            'orcid': author.get('orcid', ''),
                            'works_count': author.get('works_count', 0),
                            'cited_by_count': author.get('cited_by_count', 0)
                        }
                        
                        authors_data.append(author_data)
                        
                        # Обработка связи с организациями
                        institutions = author.get('last_known_institutions', [])
                        
                        # Отладочная информация для первых 10 авторов
                        if matched_authors <= 10:
                            logger.info(f"Автор {matched_authors}, ID: {author_id}")
                            logger.info(f"last_known_institutions: {institutions}")
                        
                        if not institutions:
                            if matched_authors <= 100:
                                logger.debug(f"Автор без институций: {author_id}")
                            continue
                        
                        for inst in institutions:
                            if isinstance(inst, dict) and 'id' in inst:
                                raw_institution_id = inst['id']
                                institution_id = normalize_id(raw_institution_id)
                                
                                # Отладочная информация для первых 10 авторов с организациями
                                if authors_with_institutions < 10:
                                    logger.info(f"Автор {author_id} связан с организацией: {raw_institution_id} -> {institution_id}")
                                    logger.info(f"Организация в списке: {institution_id in institution_ids}")
                                
                                if institution_id in institution_ids:
                                    authors_with_institutions += 1
                                    author_institution_relations.append({
                                        'author_id': author_id,
                                        'institution_id': institution_id
                                    })
                                    
                                    # Отладочная информация каждые 10 авторов с организациями
                                    if authors_with_institutions % 10 == 0:
                                        logger.info(f"Найдено {authors_with_institutions} авторов с организациями")
                        
                        # Проверка ограничения на количество авторов
                        if MAX_AUTHORS is not None and len(authors_data) >= MAX_AUTHORS:
                            logger.info(f"Достигнуто ограничение на количество авторов: {MAX_AUTHORS}")
                            break
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке автора: {str(e)}")
                    continue
        
        # Если достигнуто ограничение, прекращаем обработку файлов
        if MAX_AUTHORS is not None and len(authors_data) >= MAX_AUTHORS:
            break
    
    # Выводим статистику соответствия ID
    logger.info(f"Всего авторов обработано: {total_authors}, соответствует фильтру: {matched_authors}")
    logger.info(f"Всего авторов с организациями: {authors_with_institutions}")
    
    if author_institution_relations:
        logger.info(f"Примеры связей автор-организация: {author_institution_relations[:3]}")
    else:
        logger.info("Не найдено связей автор-организация!")
    
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
    
    # Ищем файлы с шаблоном YYYY-MM-DD_part_*.jsonl.gz
    institution_files = sorted([f for f in os.listdir(institutions_dir) if f.endswith(".jsonl.gz") and "_part_" in f])
    
    logger.info(f"Начало обработки организаций")
    logger.info(f"Найдено {len(institution_files)} файлов организаций")
    
    # Счетчики для отладки
    total_institutions = 0
    matched_institutions = 0
    
    # Выводим примеры ID из entity_ids для проверки
    logger.info(f"Примеры ID из institution_ids: {list(institution_ids)[:5] if institution_ids else []}")
    
    for institution_file in institution_files:
        file_path = os.path.join(institutions_dir, institution_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {institution_file}"):
                try:
                    institution = json.loads(line)
                    raw_institution_id = institution.get('id')
                    institution_id = normalize_id(raw_institution_id)
                    
                    total_institutions += 1
                    
                    # Отладочная информация для первых 5 организаций
                    if total_institutions <= 5:
                        logger.info(f"Организация {total_institutions}, ID: {raw_institution_id} -> {institution_id}")
                    
                    if institution_id in institution_ids:
                        matched_institutions += 1
                        
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
                        
                        # Проверка ограничения на количество организаций
                        if MAX_INSTITUTIONS is not None and len(institutions_data) >= MAX_INSTITUTIONS:
                            logger.info(f"Достигнуто ограничение на количество организаций: {MAX_INSTITUTIONS}")
                            break
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке организации: {str(e)}")
                    continue
        
        # Если достигнуто ограничение, прекращаем обработку файлов
        if MAX_INSTITUTIONS is not None and len(institutions_data) >= MAX_INSTITUTIONS:
            break
    
    # Выводим статистику
    logger.info(f"Всего организаций обработано: {total_institutions}, соответствует фильтру: {matched_institutions}")
    
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
    
    # Ищем файлы с шаблоном YYYY-MM-DD_part_*.jsonl.gz
    concept_files = sorted([f for f in os.listdir(concepts_dir) if f.endswith(".jsonl.gz") and "_part_" in f])
    
    logger.info(f"Начало обработки концепций")
    logger.info(f"Найдено {len(concept_files)} файлов концепций")
    
    # Счетчики для отладки
    total_concepts = 0
    matched_concepts = 0
    concepts_with_ancestors = 0
    
    # Выводим примеры ID из entity_ids для проверки
    logger.info(f"Примеры ID из concept_ids: {list(concept_ids)[:5] if concept_ids else []}")
    
    for concept_file in concept_files:
        file_path = os.path.join(concepts_dir, concept_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {concept_file}"):
                try:
                    concept = json.loads(line)
                    raw_concept_id = concept.get('id')
                    concept_id = normalize_id(raw_concept_id)
                    
                    total_concepts += 1
                    
                    # Отладочная информация для первых 5 концепций
                    if total_concepts <= 5:
                        logger.info(f"Концепция {total_concepts}, ID: {raw_concept_id} -> {concept_id}")
                    
                    if concept_id in concept_ids:
                        matched_concepts += 1
                        
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
                            raw_ancestor_id = ancestor.get('id')
                            ancestor_id = normalize_id(raw_ancestor_id)
                            
                            if ancestor_id:
                                concepts_with_ancestors += 1
                                concept_ids.add(ancestor_id)  # Добавляем предков в множество концепций
                                concept_ancestor_relations.append({
                                    'concept_id': concept_id,
                                    'ancestor_id': ancestor_id
                                })
                                
                                # Отладочная информация для первых 5 связей
                                if concepts_with_ancestors <= 5:
                                    logger.info(f"Связь концепция-предок: {concept_id} -> {ancestor_id}")
                        
                        # Проверка ограничения на количество концепций
                        if MAX_CONCEPTS is not None and len(concepts_data) >= MAX_CONCEPTS:
                            logger.info(f"Достигнуто ограничение на количество концепций: {MAX_CONCEPTS}")
                            break
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке концепции: {str(e)}")
                    continue
        
        # Если достигнуто ограничение, прекращаем обработку файлов
        if MAX_CONCEPTS is not None and len(concepts_data) >= MAX_CONCEPTS:
            break
    
    # Выводим статистику
    logger.info(f"Всего концепций обработано: {total_concepts}, соответствует фильтру: {matched_concepts}")
    logger.info(f"Найдено {concepts_with_ancestors} связей концепция-предок")
    
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
    
    # Ищем файлы с шаблоном YYYY-MM-DD_part_*.jsonl.gz
    source_files = sorted([f for f in os.listdir(sources_dir) if f.endswith(".jsonl.gz") and "_part_" in f])
    
    logger.info(f"Начало обработки источников (sources)")
    logger.info(f"Найдено {len(source_files)} файлов источников")
    
    # Счетчики для отладки
    total_sources = 0
    matched_sources = 0
    sources_with_publishers = 0
    
    # Выводим примеры ID из entity_ids для проверки
    logger.info(f"Примеры ID из source_ids: {list(source_ids)[:5] if source_ids else []}")
    logger.info(f"Примеры publisher_names: {list(publisher_names)[:5] if publisher_names else []}")
    
    for source_file in source_files:
        file_path = os.path.join(sources_dir, source_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {source_file}"):
                try:
                    source = json.loads(line)
                    raw_source_id = source.get('id')
                    source_id = normalize_id(raw_source_id)
                    
                    total_sources += 1
                    
                    # Отладочная информация для первых 5 источников
                    if total_sources <= 5:
                        logger.info(f"Источник {total_sources}, ID: {raw_source_id} -> {source_id}")
                    
                    if source_id in source_ids:
                        matched_sources += 1
                        
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
                            sources_with_publishers += 1
                            source_publisher_relations.append({
                                'source_id': source_id,
                                'publisher_name': publisher
                            })
                            
                            # Отладочная информация для первых 5 связей
                            if sources_with_publishers <= 5:
                                logger.info(f"Связь источник-издатель: {source_id} -> {publisher}")
                        
                        # Проверка ограничения на количество источников
                        if MAX_SOURCES is not None and len(sources_data) >= MAX_SOURCES:
                            logger.info(f"Достигнуто ограничение на количество источников: {MAX_SOURCES}")
                            break
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке источника: {str(e)}")
                    continue
        
        # Если достигнуто ограничение, прекращаем обработку файлов
        if MAX_SOURCES is not None and len(sources_data) >= MAX_SOURCES:
            break
    
    # Выводим статистику
    logger.info(f"Всего источников обработано: {total_sources}, соответствует фильтру: {matched_sources}")
    logger.info(f"Найдено {sources_with_publishers} связей источник-издатель")
    
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
    
    # Ищем файлы с шаблоном YYYY-MM-DD_part_*.jsonl.gz
    publisher_files = sorted([f for f in os.listdir(publishers_dir) if f.endswith(".jsonl.gz") and "_part_" in f])
    
    logger.info(f"Начало обработки издателей")
    logger.info(f"Найдено {len(publisher_files)} файлов издателей")
    
    for publisher_file in publisher_files:
        file_path = os.path.join(publishers_dir, publisher_file)
        logger.info(f"Обработка файла: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Обработка {publisher_file}"):
                try:
                    publisher = json.loads(line)
                    publisher_name = publisher.get('display_name')  # Не применяем normalize_id к имени издателя
                    
                    if publisher_name in publisher_names:
                        # Извлечение данных об издателе
                        publisher_data = {
                            'name': publisher_name,
                            'works_count': publisher.get('works_count', 0),
                            'cited_by_count': publisher.get('cited_by_count', 0),
                            'country_codes': ','.join(publisher.get('country_codes', []))
                        }
                        
                        publishers_data.append(publisher_data)
                        
                        # Проверка ограничения на количество издателей
                        if MAX_PUBLISHERS is not None and len(publishers_data) >= MAX_PUBLISHERS:
                            logger.info(f"Достигнуто ограничение на количество издателей: {MAX_PUBLISHERS}")
                            break
                
                except Exception as e:
                    logger.error(f"Ошибка при обработке издателя: {str(e)}")
                    continue
        
        # Если достигнуто ограничение, прекращаем обработку файлов
        if MAX_PUBLISHERS is not None and len(publishers_data) >= MAX_PUBLISHERS:
            break
    
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
    process_authors(entity_ids["author_ids"], entity_ids)
    
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