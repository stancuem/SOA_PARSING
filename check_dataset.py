import os
import pandas as pd
import logging
import json
import time
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("check.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("checker")

# Директории для данных
OUTPUT_DIR = "output"

# Функция для расчета размера файла в МБ
def get_file_size_mb(file_path):
    return os.path.getsize(file_path) / (1024 * 1024)

# Функция для проверки объема данных
def check_dataset_size():
    logger.info("Проверка объема данных...")
    
    total_size_mb = 0
    file_stats = []
    
    # Проверка всех CSV-файлов в выходной директории
    for file_name in os.listdir(OUTPUT_DIR):
        if file_name.endswith('.csv'):
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            # Расчет размера файла
            size_mb = get_file_size_mb(file_path)
            total_size_mb += size_mb
            
            # Подсчет количества строк
            df = pd.read_csv(file_path)
            row_count = len(df)
            
            file_stats.append({
                'file_name': file_name,
                'size_mb': size_mb,
                'row_count': row_count
            })
            
            logger.info(f"Файл: {file_name}, Размер: {size_mb:.2f} МБ, Строк: {row_count}")
    
    logger.info(f"Общий размер датасета: {total_size_mb:.2f} МБ ({total_size_mb/1024:.2f} ГБ)")
    
    # Проверка на соответствие требуемому размеру (5-15 ГБ)
    if total_size_mb < 5 * 1024:
        logger.warning(f"Размер датасета меньше рекомендуемого (5 ГБ)")
    elif total_size_mb > 15 * 1024:
        logger.warning(f"Размер датасета больше рекомендуемого (15 ГБ)")
    else:
        logger.info(f"Размер датасета соответствует требованиям (5-15 ГБ)")
    
    return file_stats, total_size_mb

# Функция для проверки связности данных
def check_dataset_consistency():
    logger.info("Проверка связности данных...")
    
    # Загрузка всех CSV-файлов
    dfs = {}
    for file_name in os.listdir(OUTPUT_DIR):
        if file_name.endswith('.csv'):
            file_path = os.path.join(OUTPUT_DIR, file_name)
            dfs[file_name[:-4]] = pd.read_csv(file_path)
    
    consistency_issues = []
    
    # Проверка связей между работами и авторами
    if 'works' in dfs and 'author_work' in dfs and 'authors' in dfs:
        # Проверка, что все work_id в author_work существуют в works
        work_ids_in_works = set(dfs['works']['id'])
        work_ids_in_author_work = set(dfs['author_work']['work_id'])
        missing_work_ids = work_ids_in_author_work - work_ids_in_works
        
        if missing_work_ids:
            issue = f"Найдены {len(missing_work_ids)} ID работ в author_work, которых нет в works"
            consistency_issues.append(issue)
            logger.warning(issue)
        
        # Проверка, что все author_id в author_work существуют в authors
        author_ids_in_authors = set(dfs['authors']['id'])
        author_ids_in_author_work = set(dfs['author_work']['author_id'])
        missing_author_ids = author_ids_in_author_work - author_ids_in_authors
        
        if missing_author_ids:
            issue = f"Найдены {len(missing_author_ids)} ID авторов в author_work, которых нет в authors"
            consistency_issues.append(issue)
            logger.warning(issue)
    
    # Проверка связей между работами и концепциями
    if 'works' in dfs and 'work_concept' in dfs and 'concepts' in dfs:
        # Проверка, что все work_id в work_concept существуют в works
        work_ids_in_works = set(dfs['works']['id'])
        work_ids_in_work_concept = set(dfs['work_concept']['work_id'])
        missing_work_ids = work_ids_in_work_concept - work_ids_in_works
        
        if missing_work_ids:
            issue = f"Найдены {len(missing_work_ids)} ID работ в work_concept, которых нет в works"
            consistency_issues.append(issue)
            logger.warning(issue)
        
        # Проверка, что все concept_id в work_concept существуют в concepts
        concept_ids_in_concepts = set(dfs['concepts']['id'])
        concept_ids_in_work_concept = set(dfs['work_concept']['concept_id'])
        missing_concept_ids = concept_ids_in_work_concept - concept_ids_in_concepts
        
        if missing_concept_ids:
            issue = f"Найдены {len(missing_concept_ids)} ID концепций в work_concept, которых нет в concepts"
            consistency_issues.append(issue)
            logger.warning(issue)
    
    # Проверка связей между работами и источниками
    if 'works' in dfs and 'work_source' in dfs and 'sources' in dfs:
        # Проверка, что все work_id в work_source существуют в works
        work_ids_in_works = set(dfs['works']['id'])
        work_ids_in_work_source = set(dfs['work_source']['work_id'])
        missing_work_ids = work_ids_in_work_source - work_ids_in_works
        
        if missing_work_ids:
            issue = f"Найдены {len(missing_work_ids)} ID работ в work_source, которых нет в works"
            consistency_issues.append(issue)
            logger.warning(issue)
        
        # Проверка, что все source_id в work_source существуют в sources
        source_ids_in_sources = set(dfs['sources']['id'])
        source_ids_in_work_source = set(dfs['work_source']['source_id'])
        missing_source_ids = source_ids_in_work_source - source_ids_in_sources
        
        if missing_source_ids:
            issue = f"Найдены {len(missing_source_ids)} ID источников в work_source, которых нет в sources"
            consistency_issues.append(issue)
            logger.warning(issue)
    
    # Проверка связей между авторами и организациями
    if 'authors' in dfs and 'author_institution' in dfs and 'institutions' in dfs:
        # Проверка, что все author_id в author_institution существуют в authors
        author_ids_in_authors = set(dfs['authors']['id'])
        author_ids_in_author_institution = set(dfs['author_institution']['author_id'])
        missing_author_ids = author_ids_in_author_institution - author_ids_in_authors
        
        if missing_author_ids:
            issue = f"Найдены {len(missing_author_ids)} ID авторов в author_institution, которых нет в authors"
            consistency_issues.append(issue)
            logger.warning(issue)
        
        # Проверка, что все institution_id в author_institution существуют в institutions
        institution_ids_in_institutions = set(dfs['institutions']['id'])
        institution_ids_in_author_institution = set(dfs['author_institution']['institution_id'])
        missing_institution_ids = institution_ids_in_author_institution - institution_ids_in_institutions
        
        if missing_institution_ids:
            issue = f"Найдены {len(missing_institution_ids)} ID организаций в author_institution, которых нет в institutions"
            consistency_issues.append(issue)
            logger.warning(issue)
    
    # Проверка связей между концепциями и их предками
    if 'concepts' in dfs and 'concept_ancestor' in dfs:
        # Проверка, что все concept_id в concept_ancestor существуют в concepts
        concept_ids_in_concepts = set(dfs['concepts']['id'])
        concept_ids_in_concept_ancestor = set(dfs['concept_ancestor']['concept_id'])
        missing_concept_ids = concept_ids_in_concept_ancestor - concept_ids_in_concepts
        
        if missing_concept_ids:
            issue = f"Найдены {len(missing_concept_ids)} ID концепций в concept_ancestor, которых нет в concepts"
            consistency_issues.append(issue)
            logger.warning(issue)
        
        # Проверка, что все ancestor_id в concept_ancestor существуют в concepts
        ancestor_ids_in_concept_ancestor = set(dfs['concept_ancestor']['ancestor_id'])
        missing_ancestor_ids = ancestor_ids_in_concept_ancestor - concept_ids_in_concepts
        
        if missing_ancestor_ids:
            issue = f"Найдены {len(missing_ancestor_ids)} ID предков в concept_ancestor, которых нет в concepts"
            consistency_issues.append(issue)
            logger.warning(issue)
    
    # Проверка связей между источниками и издателями
    if 'sources' in dfs and 'source_publisher' in dfs and 'publishers' in dfs:
        # Проверка, что все source_id в source_publisher существуют в sources
        source_ids_in_sources = set(dfs['sources']['id'])
        source_ids_in_source_publisher = set(dfs['source_publisher']['source_id'])
        missing_source_ids = source_ids_in_source_publisher - source_ids_in_sources
        
        if missing_source_ids:
            issue = f"Найдены {len(missing_source_ids)} ID источников в source_publisher, которых нет в sources"
            consistency_issues.append(issue)
            logger.warning(issue)
        
        # Проверка, что все publisher_name в source_publisher существуют в publishers
        publisher_names_in_publishers = set(dfs['publishers']['name'])
        publisher_names_in_source_publisher = set(dfs['source_publisher']['publisher_name'])
        missing_publisher_names = publisher_names_in_source_publisher - publisher_names_in_publishers
        
        if missing_publisher_names:
            issue = f"Найдены {len(missing_publisher_names)} имен издателей в source_publisher, которых нет в publishers"
            consistency_issues.append(issue)
            logger.warning(issue)
    
    if not consistency_issues:
        logger.info("Проверка связности завершена. Проблем не обнаружено.")
    else:
        logger.warning(f"Проверка связности завершена. Обнаружено {len(consistency_issues)} проблем.")
    
    return consistency_issues

# Основная функция проверки датасета
def check_dataset():
    start_time = time.time()
    
    # Проверка наличия выходной директории
    if not os.path.exists(OUTPUT_DIR):
        logger.error(f"Выходная директория {OUTPUT_DIR} не существует.")
        return
    
    # Проверка объема данных
    file_stats, total_size_mb = check_dataset_size()
    
    # Проверка связности данных
    consistency_issues = check_dataset_consistency()
    
    # Создание метаданных
    metadata = {
        'total_size_mb': total_size_mb,
        'total_size_gb': total_size_mb / 1024,
        'file_stats': file_stats,
        'consistency_issues': consistency_issues,
        'check_time': time.strftime('%Y-%m-%d %H:%M:%S'),
        'processing_time_seconds': time.time() - start_time
    }
    
    # Сохранение метаданных
    with open(os.path.join(OUTPUT_DIR, 'metadata.json'), 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Метаданные сохранены в {os.path.join(OUTPUT_DIR, 'metadata.json')}")
    logger.info(f"Проверка датасета завершена за {metadata['processing_time_seconds']:.2f} секунд")
    
    return metadata

if __name__ == "__main__":
    check_dataset() 