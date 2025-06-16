import os
import logging
import time
import argparse
from download_data import download_data
from process_works import process_works
from process_entities import process_entities
from check_dataset import check_dataset

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("semopenalex.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("semopenalex")

def get_user_confirmation(step_name):
    """Запрашивает у пользователя подтверждение для выполнения следующего шага."""
    while True:
        response = input(f"\nПриступить к выполнению шага '{step_name}'? (да/нет/пропустить): ").strip().lower()
        if response in ['да', 'yes', 'y', 'д']:
            return True
        elif response in ['нет', 'no', 'n', 'н']:
            return False
        elif response in ['пропустить', 'skip', 's', 'п']:
            logger.info(f"Шаг '{step_name}' пропущен по запросу пользователя")
            return None
        else:
            print("Пожалуйста, введите 'да', 'нет' или 'пропустить'")

def main():
    parser = argparse.ArgumentParser(description='Создание датасета SemOpenAlex из OpenAlex S3 с ограничением по объёму')
    parser.add_argument('--non-interactive', action='store_true', help='Запустить в неинтерактивном режиме (без запросов подтверждения)')
    parser.add_argument('--skip-download', action='store_true', help='Пропустить загрузку данных')
    parser.add_argument('--skip-works', action='store_true', help='Пропустить обработку публикаций')
    parser.add_argument('--skip-entities', action='store_true', help='Пропустить обработку связанных сущностей')
    parser.add_argument('--skip-check', action='store_true', help='Пропустить проверку датасета')
    parser.add_argument('--max-works', type=int, default=100000, help='Максимальное количество публикаций для обработки')
    args = parser.parse_args()
    
    start_time = time.time()
    logger.info("Начало создания датасета SemOpenAlex")
    
    # Определяем, интерактивный режим или нет
    interactive_mode = not args.non_interactive
    
    # Шаг 1: Загрузка данных из OpenAlex S3
    if not args.skip_download:
        if not interactive_mode or get_user_confirmation("Загрузка данных из OpenAlex S3") is True:
            logger.info("Шаг 1: Загрузка данных из OpenAlex S3")
            download_data()
            logger.info("Шаг 1 завершен: Данные загружены из OpenAlex S3")
        else:
            logger.info("Шаг 1: Загрузка данных пропущена по запросу пользователя")
    else:
        logger.info("Шаг 1: Загрузка данных пропущена (--skip-download)")
    
    # Шаг 2: Обработка публикаций (works)
    if not args.skip_works:
        if not interactive_mode or get_user_confirmation("Обработка публикаций (works)") is True:
            logger.info("Шаг 2: Обработка публикаций (works)")
            # Изменение максимального количества публикаций
            import process_works
            process_works.MAX_WORKS = args.max_works
            process_works.process_works()
            logger.info("Шаг 2 завершен: Публикации обработаны")
        else:
            logger.info("Шаг 2: Обработка публикаций пропущена по запросу пользователя")
    else:
        logger.info("Шаг 2: Обработка публикаций пропущена (--skip-works)")
    
    # Шаг 3: Обработка связанных сущностей
    if not args.skip_entities:
        if not interactive_mode or get_user_confirmation("Обработка связанных сущностей") is True:
            logger.info("Шаг 3: Обработка связанных сущностей")
            process_entities()
            logger.info("Шаг 3 завершен: Связанные сущности обработаны")
        else:
            logger.info("Шаг 3: Обработка связанных сущностей пропущена по запросу пользователя")
    else:
        logger.info("Шаг 3: Обработка связанных сущностей пропущена (--skip-entities)")
    
    # Шаг 4: Проверка датасета
    if not args.skip_check:
        if not interactive_mode or get_user_confirmation("Проверка датасета") is True:
            logger.info("Шаг 4: Проверка датасета")
            metadata = check_dataset()
            
            # Вывод итоговой статистики
            if metadata:
                logger.info("Итоговая статистика датасета:")
                logger.info(f"Общий размер: {metadata.get('total_size_gb', 0):.2f} ГБ")
                logger.info(f"Количество файлов: {len(metadata.get('file_stats', []))}")
                
                # Проверка на проблемы связности
                if metadata.get('consistency_issues'):
                    logger.warning(f"Обнаружены проблемы связности: {len(metadata.get('consistency_issues', []))}")
                else:
                    logger.info("Проблем связности не обнаружено")
            logger.info("Шаг 4 завершен: Датасет проверен")
        else:
            logger.info("Шаг 4: Проверка датасета пропущена по запросу пользователя")
    else:
        logger.info("Шаг 4: Проверка датасета пропущена (--skip-check)")
    
    # Итоговое время выполнения
    end_time = time.time()
    total_time = end_time - start_time
    hours, remainder = divmod(total_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    logger.info(f"Создание датасета SemOpenAlex завершено за {int(hours)}:{int(minutes):02}:{int(seconds):02}")
    logger.info("Датасет готов для импорта в PostgreSQL, Neo4j и ClickHouse")
    
    if interactive_mode:
        print("\nСоздание датасета SemOpenAlex завершено!")
        print(f"Общее время выполнения: {int(hours)}:{int(minutes):02}:{int(seconds):02}")
        print("Датасет готов для импорта в PostgreSQL, Neo4j и ClickHouse")

if __name__ == "__main__":
    main() 