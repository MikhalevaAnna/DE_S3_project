"""
Упрощенный запуск пайплайна с фильтрацией по зарплате.
"""
import sys
import os
import asyncio
from pathlib import Path
from datetime import datetime
import logging
import traceback

# Подавляем предупреждения
import warnings

warnings.filterwarnings('ignore')
import urllib3

urllib3.disable_warnings()

# Импорты
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from async_s3_client import AsyncObjectStorage
from pipeline import DataPipeline
from config import config


# Настройка логирования
def setup_logging():
    """Настройка логирования в файл и консоль"""
    logs_dir = Path(config.PIPELINE_CONFIG['log_folder'])
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Формируем имя файла с датой
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'pipeline_task3_{timestamp}.log'

    # Создаем логгер
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Очищаем существующие обработчики
    logger.handlers.clear()

    # Форматтеры
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter('%(message)s')

    # Обработчик для файла
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Обработчик для консоли (с фильтром для вывода только ключевых сообщений)
    class ConsoleFilter(logging.Filter):
        def filter(self, record):
            # В консоль выводим только важные сообщения:
            # - Запуск/остановка программы
            # - Ошибки
            # - Ключевые этапы обработки файлов
            # - Статистику
            key_phrases = [
                'ПАЙПЛАЙН', 'ЗАВЕРШЕН', 'ОШИБКА', 'УСПЕШНО', 'ОБНАРУЖЕН',
                'Файл логов', 'Подключение успешно', 'Пример файла создан',
                'Статистика:', 'Загружен в S3', 'МОНИТОРИНГ'
            ]
            message = record.getMessage()
            return any(phrase in message for phrase in key_phrases) or record.levelno >= logging.WARNING

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(ConsoleFilter())

    # Добавляем обработчики
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Отключаем логи urllib3 и botocore
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    # Возвращаем логгер и путь к файлу логов
    return logger, str(log_file.absolute())


# Настраиваем логирование
logger, log_file_path = setup_logging()

# Выводим ключевую информацию в консоль ПЕРЕД запуском
print("\n" + "=" * 70)
print("🚀 ЗАПУСК ПАЙПЛАЙНА ОБРАБОТКИ ДАННЫХ")
print("=" * 70)
print(f"📁 Папка наблюдения: {config.PIPELINE_CONFIG['watch_folder']}")
print(f"🎯 Фильтрация: зарплата > {config.PIPELINE_CONFIG['filter_threshold']}")
print(f"📄 Логи будут сохранены: {log_file_path}")
print("=" * 70)
print("⏹️  Для остановки нажмите Ctrl+C\n")


async def monitor_and_process(pipeline: DataPipeline, watch_folder: Path, check_interval: int = 3):
    """Мониторинг и обработка файлов."""
    processed_files = set()

    logger.info(f"\n👁️  Мониторинг папки: {watch_folder.absolute()}")
    logger.info(f"🎯 Фильтрация: зарплата > {config.PIPELINE_CONFIG['filter_threshold']}")
    logger.info("📋 Поддерживаемые форматы: CSV, JSON, Excel, Parquet, TXT")
    logger.info("⏹️  Для остановки нажмите Ctrl+C\n")
    logger.info("=" * 70)

    try:
        while True:
            # Сканируем папку
            current_time = datetime.now().strftime('%H:%M:%S')
            files = list(watch_folder.glob("*.*"))
            unprocessed_files = []

            for file_path in files:
                if not file_path.is_file():
                    continue

                # Проверяем расширение
                valid_ext = {'.csv', '.json', '.xlsx', '.xls', '.parquet', '.txt'}
                if file_path.suffix.lower() not in valid_ext:
                    continue

                # Пропускаем временные файлы
                if file_path.name.startswith(('.', '~', 'temp_', 'salary_filtered_')):
                    continue

                file_key = str(file_path.resolve())

                if file_key not in processed_files:
                    unprocessed_files.append(file_path)

            # Показываем статус в консоль
            print(f"\n⏰ {current_time} | 📁 Файлов в папке: {len(files)} | ⏳ Новых: {len(unprocessed_files)}")

            # Обрабатываем новые файлы
            for file_path in unprocessed_files:
                file_key = str(file_path.resolve())
                processed_files.add(file_key)

                # Выводим в консоль обнаружение нового файла
                print(f"\n{'=' * 60}")
                print(f"📁 ОБНАРУЖЕН НОВЫЙ ФАЙЛ: {file_path.name}")
                print(f"{'=' * 60}")

                logger.info(f"\n{'=' * 70}")
                logger.info(f"📁 ОБНАРУЖЕН НОВЫЙ ФАЙЛ: {file_path.name}")
                logger.info(f"{'=' * 70}")

                # Проверяем, что файл полностью записан
                print("🔍 Проверка файла...")
                logger.info("🔍 Проверка файла...")
                size1 = file_path.stat().st_size
                await asyncio.sleep(1)
                size2 = file_path.stat().st_size

                if size1 != size2 or size1 == 0:
                    print("   ⚠️ Файл еще записывается, пропускаем...")
                    logger.info("   ⚠️ Файл еще записывается, пропускаем...")
                    processed_files.remove(file_key)
                    continue

                # Обрабатываем файл
                print("🔄 Начало обработки...")
                logger.info("🔄 Начало обработки...")
                result = await pipeline.process_file(file_path)
                await pipeline.log_pipeline_result(result)

                if result['success']:
                    # Выводим результат в консоль
                    print(f"\n✅ ФАЙЛ ОБРАБОТАН УСПЕШНО!")
                    print(f"{'-' * 50}")
                    print(f"📊 Статистика:")
                    print(f"   Всего записей: {result.get('records_processed', 0)}")
                    print(f"   Отфильтровано по зарплате (≤ {config.PIPELINE_CONFIG['filter_threshold']}): "
                          f"{result.get('filtered_by_salary', 0)}")
                    print(f"   Осталось записей: {result.get('records_filtered', 0)}")
                    print(f"📤 Результат:")
                    print(f"   Загружен в S3: {result.get('s3_path', 'N/A')}")
                    if result.get('version_id') and result.get('version_id') != 'unknown':
                        print(f"   Версия: {result.get('version_id', 'N/A')[:12]}...")
                    print(f"🗂️  Исходный файл перемещен в архив")
                    print(f"{'=' * 60}")

                    # Также логируем полную информацию
                    logger.info(f"\n✅ ФАЙЛ ОБРАБОТАН УСПЕШНО!")
                    logger.info(f"{'-' * 40}")
                    logger.info(f"📊 Статистика:")
                    logger.info(f"   Всего записей: {result.get('records_processed', 0)}")
                    logger.info(f"   Отфильтровано по зарплате (≤ {config.PIPELINE_CONFIG['filter_threshold']}): "
                                f"{result.get('filtered_by_salary', 0)}")
                    logger.info(f"   Осталось записей: {result.get('records_filtered', 0)}")
                    logger.info(f"📤 Результат:")
                    logger.info(f"   Загружен в S3: {result.get('s3_path', 'N/A')}")
                    logger.info(f"   Версия: {result.get('version_id', 'N/A')}")
                    logger.info(f"🗂️  Исходный файл:")
                    logger.info(f"   Перемещен в: processed/archive/")
                    logger.info(f"{'=' * 70}")
                else:
                    # Выводим ошибку в консоль
                    print(f"\n❌ ОШИБКА ОБРАБОТКИ!")
                    print(f"{'-' * 50}")
                    print(f"   Причина: {result.get('error', 'Неизвестная ошибка')}")
                    print(f"{'=' * 60}")

                    # Также логируем
                    logger.error(f"\n❌ ОШИБКА ОБРАБОТКИ:")
                    logger.error(f"{'-' * 40}")
                    logger.error(f"   Причина: {result.get('error', 'Неизвестная ошибка')}")
                    logger.info(f"{'=' * 70}")

            # Ждем перед следующей проверкой
            await asyncio.sleep(check_interval)

    except KeyboardInterrupt:
        print("\n\n🛑 Получен сигнал остановки...")
        logger.info("\n\n🛑 Получен сигнал остановки...")
    except Exception as e:
        print(f"\n💥 Ошибка мониторинга: {e}")
        logger.error(f"\n💥 Ошибка мониторинга: {e}")
        logger.error(traceback.format_exc())


async def main():
    """Основная функция."""
    # Выводим заголовок в консоль
    print("\n" + "=" * 70)
    print("🎯 ПАЙПЛАЙН ОБРАБОТКИ ФАЙЛОВ С ФИЛЬТРАЦИЕЙ ПО ЗАРПЛАТЕ")
    print("=" * 70)
    print("📝 Фильтрация: данные попадают в отфильтрованный файл,")
    print(f"   если зарплата больше {config.PIPELINE_CONFIG['filter_threshold']}")
    print("=" * 70)

    # Логируем ту же информацию
    logger.info("=" * 70)
    logger.info("🎯 ПАЙПЛАЙН ОБРАБОТКИ ФАЙЛОВ С ФИЛЬТРАЦИЕЙ ПО ЗАРПЛАТЕ")
    logger.info("=" * 70)
    logger.info("📝 Фильтрация: данные попадают в отфильтрованный файл,")
    logger.info(f"   если зарплата больше {config.PIPELINE_CONFIG['filter_threshold']}")
    logger.info("=" * 70)

    # Выводим информацию о логировании
    print(f"📁 Файл логов: {log_file_path}")
    logger.info(f"📁 Файл логов: {log_file_path}")

    try:
        # Инициализация клиента
        print("\n🔧 Инициализация S3 клиента...")
        logger.info("\n🔧 Инициализация S3 клиента...")
        
        client = AsyncObjectStorage(
            key_id=config.S3_CONFIG['access_key'],
            secret=config.S3_CONFIG['secret_key'],
            endpoint=config.PIPELINE_CONFIG.get('endpoint', config.S3_CONFIG['endpoint']),
            container=config.PIPELINE_CONFIG.get('container', config.S3_CONFIG['bucket']),
            region=config.PIPELINE_CONFIG.get('region', config.S3_CONFIG.get('region', 'ru-1')),
            verify_ssl=config.PIPELINE_CONFIG.get('verify_ssl', config.S3_CONFIG.get('verify_ssl', False))
        )

        # Тестирование подключения
        print("🔍 Тестирование подключения к S3...")
        logger.info("🔍 Тестирование подключения к S3...")
        try:
            files = await client.list_files()
            print(f"   ✅ Подключение успешно!")
            print(f"   📁 Файлов в бакете: {len(files)}")
            if files:
                print(f"   📋 Примеры файлов:")
                for i, f in enumerate(files[:3], 1):
                    print(f"     {i}. {f}")
            
            logger.info(f"   ✅ Подключение успешно!")
            logger.info(f"   📁 Файлов в бакете: {len(files)}")
            if files:
                logger.info(f"   📋 Примеры файлов:")
                for i, f in enumerate(files[:3], 1):
                    logger.info(f"     {i}. {f}")
        except Exception as e:
            print(f"   ⚠️ Предупреждение: {e}")
            print("   Продолжаем работу...")
            logger.warning(f"   ⚠️ Предупреждение: {e}")
            logger.info("   Продолжаем работу...")

        # Создание пайплайна
        print("\n🔧 Создание пайплайна...")
        logger.info("\n🔧 Создание пайплайна...")
        pipeline = DataPipeline(client, config.PIPELINE_CONFIG)

        # Проверка папки watch
        print("\n🔍 Проверка папки incoming...")
        logger.info("\n🔍 Проверка папки incoming...")
        print(f"   Путь: {config.PIPELINE_CONFIG['watch_folder']}")
        logger.info(f"   Путь из конфига: {config.PIPELINE_CONFIG['watch_folder']}")

        watch_folder = Path(config.PIPELINE_CONFIG['watch_folder'])
        logger.info(f"   Абсолютный путь: {watch_folder.absolute()}")

        # Создаем папку
        try:
            watch_folder.mkdir(parents=True, exist_ok=True)
            print(f"   ✅ Папка создана/существует")
            logger.info(f"   ✅ Папка создана/существует")
        except Exception as e:
            print(f"   ❌ Ошибка создания папки: {e}")
            logger.error(f"   ❌ Ошибка создания папки: {e}")

        # Проверяем существование папки
        if watch_folder.exists():
            print(f"   ✅ Папка существует")
            logger.info(f"   ✅ Папка существует")
        else:
            print(f"   ❌ Папка не существует!")
            logger.error(f"   ❌ Папка не существует!")

        # Создание примера файла если папка пуста
        example_files = list(watch_folder.glob("*.*"))
        print(f"   📊 Найдено файлов в папке: {len(example_files)}")
        logger.info(f"   📊 Найдено файлов в папке: {len(example_files)}")

        if not example_files:
            print("   📭 Папка incoming пуста")
            print("   📝 Создаю пример файла с данными...")
            logger.info("   📭 Папка incoming пуста")
            logger.info("   📝 Создаю пример файла с данными...")

            example_file = watch_folder / "employees_example.csv"
            example_content = """id,name,department,position,salary,hire_date,city
1,Иван Иванов,IT,Разработчик,50000,2023-01-15,Москва
2,Петр Петров,Маркетинг,Менеджер,80000,2022-06-20,Санкт-Петербург
3,Мария Сидорова,Финансы,Аналитик,60000,2023-03-10,Москва
4,Анна Кузнецова,HR,Специалист,45000,2023-05-05,Казань
5,Алексей Смирнов,IT,Тимлид,120000,2021-11-30,Москва
6,Елена Попова,Продажи,Менеджер,55000,2022-09-15,Новосибирск
7,Дмитрий Васильев,IT,Тестировщик,40000,2023-07-20,Москва
8,Ольга Новикова,Маркетинг,Дизайнер,48000,2023-02-28,Екатеринбург
9,Сергей Морозов,Финансы,Директор,150000,2020-04-10,Москва
10,Наталья Воробьева,HR,Менеджер,52000,2022-12-01,Краснодар
11,Андрей Павлов,IT,Стажер,80,2023-10-01,Москва
12,Екатерина Лебедева,Продажи,Стажер,90,2023-09-15,Санкт-Петербург
13,Максим Козлов,IT,Разработчик,95000,2022-03-15,Новосибирск
14,Ольга Соколова,Финансы,Бухгалтер,35000,2023-04-20,Казань
15,Денис Орлов,Маркетинг,Копирайтер,30000,2023-06-10,Екатеринбург"""

            try:
                with open(example_file, 'w', encoding='utf-8') as f:
                    f.write(example_content)

                print(f"   ✅ Пример файла создан: {example_file.name}")
                print(f"   📊 В файле 15 записей, включая стажеров с зарплатой ≤ "
                      f"{config.PIPELINE_CONFIG['filter_threshold']}")
                print(f"   📍 Полный путь: {example_file.absolute()}")

                logger.info(f"   ✅ Пример файла создан: {example_file.name}")
                logger.info(f"   📊 В файле 15 записей, включая стажеров с зарплатой ≤ "
                            f"{config.PIPELINE_CONFIG['filter_threshold']}")
                logger.info(f"   📍 Полный путь: {example_file.absolute()}")

            except Exception as e:
                print(f"   ❌ Ошибка создания файла: {e}")
                logger.error(f"   ❌ Ошибка создания файла: {e}")
        else:
            print(f"   📁 Найдено файлов: {len(example_files)}")
            for i, file_path in enumerate(example_files, 1):
                if file_path.is_file():
                    print(f"   {i}. {file_path.name}")
            
            logger.info(f"   📁 Найдено файлов: {len(example_files)}")
            for i, file_path in enumerate(example_files, 1):
                if file_path.is_file():
                    logger.info(f"   {i}. {file_path.name}")

        # Обработка существующих файлов
        print("\n🔄 Обработка существующих файлов...")
        logger.info("\n🔄 Обработка существующих файлов...")
        await pipeline.process_existing_files()

        # Запуск мониторинга
        print("\n" + "=" * 70)
        print("🚀 ПАЙПЛАЙН ЗАПУЩЕН")
        print("=" * 70)
        print("📋 Что делает пайплайн:")
        print("   1. Находит колонку с зарплатой (salary, оклад, доход)")
        print(f"   2. Фильтрует записи: оставляет только с зарплатой > {config.PIPELINE_CONFIG['filter_threshold']}")
        print("   3. Удаляет дубликаты и пустые значения")
        print("   4. Сохраняет результат в CSV с метаданными")
        print("   5. Загружает в S3 с версионированием")
        print("   6. Перемещает исходный файл в архив")
        print("   7. Логирует все действия локально и в S3")
        print(f"\n📁 Положите файлы в папку: {watch_folder.absolute()}")
        print("⏹️  Для остановки нажмите Ctrl+C")
        print("=" * 70)
        print(f"📄 Логи пайплайна: {log_file_path}")

        logger.info("\n" + "=" * 70)
        logger.info("🚀 ПАЙПЛАЙН ЗАПУЩЕН")
        logger.info("=" * 70)
        logger.info("📋 Что делает пайплайн:")
        logger.info("   1. Находит колонку с зарплатой (salary, оклад, доход)")
        logger.info(
            f"   2. Фильтрует записи: оставляет только с зарплатой > {config.PIPELINE_CONFIG['filter_threshold']}")
        logger.info("   3. Удаляет дубликаты и пустые значения")
        logger.info("   4. Сохраняет результат в CSV с метаданными")
        logger.info("   5. Загружает в S3 с версионированием")
        logger.info("   6. Перемещает исходный файл в архив")
        logger.info("   7. Логирует все действия локально и в S3")
        logger.info(f"\n📁 Положите файлы в папку: {watch_folder.absolute()}")
        logger.info("⏹️  Для остановки нажмите Ctrl+C")
        logger.info("=" * 70)
        logger.info(f"📄 Логи пайплайна: {log_file_path}")

        await monitor_and_process(pipeline, watch_folder, check_interval=5)

    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        print(f"Подробности в логах: {log_file_path}")
        logger.error(f"\n💥 Критическая ошибка: {e}")
        logger.error(traceback.format_exc())

    print("\n" + "=" * 70)
    print("✅ ПАЙПЛАЙН ЗАВЕРШЕН")
    print(f"📅 Время завершения: {datetime.now().strftime('%H:%M:%S')}")
    print(f"📁 Логи сохранены в: {log_file_path}")
    print("=" * 70)

    logger.info("\n" + "=" * 70)
    logger.info("✅ ПАЙПЛАЙН ЗАВЕРШЕН")
    logger.info(f"📅 Время завершения: {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"📁 Логи сохранены в: {log_file_path}")
    logger.info("=" * 70)


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Выводим путь к логам при завершении
        print(f"\n📁 Файл логов сохранен: {log_file_path}")
        logger.info("\n\n👋 Программа завершена")
    except Exception as e:
        # Выводим путь к логам при ошибке
        print(f"\n💥 Фатальная ошибка! Детали в логах: {log_file_path}")
        logger.error(f"\n💥 Фатальная ошибка: {e}")
        logger.error(traceback.format_exc())
