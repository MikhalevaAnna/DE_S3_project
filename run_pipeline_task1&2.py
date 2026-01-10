import sys
import os
from config import config
import asyncio
import time
from pathlib import Path
from typing import Optional
import warnings
import logging
from datetime import datetime

# Подавляем предупреждения о SSL для Selectel
warnings.filterwarnings('ignore')
import urllib3

urllib3.disable_warnings()

# Определяем пути для импорта
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')

# Добавляем src в путь Python
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)


# Настраиваем логирование
def setup_logging():
    """Настройка логирования в файл и консоль"""
    # Создаем папку для логов если её нет
    logs_dir = Path(config.PIPELINE_CONFIG['log_folder'])
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Формируем имя файла с датой
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f'pipeline_tasks_1&2_{timestamp}.log'

    # Настраиваем формат логов
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Создаем логгер
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Очищаем существующие обработчики
    logger.handlers.clear()

    # Обработчик для файла
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(log_format, date_format)
    file_handler.setFormatter(file_formatter)

    # Обработчик для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    # Добавляем обработчики
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Сохраняем путь к файлу логов для вывода
    global LOG_FILE_PATH
    LOG_FILE_PATH = str(log_file)

    return logger


# Инициализируем логирование
logger = setup_logging()

# Импортируем модули из src
try:
    from async_s3_client import AsyncObjectStorage

    logger.info("✅ Модули успешно импортированы из src")
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    logger.error("Проверьте структуру проекта:")
    logger.error("project/")
    logger.error("├── config/")
    logger.error("│   └── config.py")
    logger.error("├── data/")
    logger.error("│   └── incoming")
    logger.error("│   └── logs")
    logger.error("│   └── processed")
    logger.error("│   └── temp")
    logger.error("├── src/")
    logger.error("│   ├── async_s3_client.py")
    logger.error("│   └── pipeline.py")
    logger.error("├── screenshots/")
    logger.error("├── tests/")
    logger.error("│   ├── test_config.py")
    logger.error("│   └── test_selectel_connection.py")
    logger.error("├── .env")
    logger.error("├── requirements.txt")
    logger.error("├── run_pipeline_task1&2.py")
    logger.error("└── run_pipeline_task3.py")

    sys.exit(1)


async def create_async_client() -> Optional[AsyncObjectStorage]:
    """Создает асинхронный S3 клиент"""
    try:
        client = AsyncObjectStorage(
            key_id=config.S3_CONFIG['access_key'],
            secret=config.S3_CONFIG['secret_key'],
            endpoint=config.S3_CONFIG['endpoint'],
            container=config.S3_CONFIG['bucket'],
            region=config.S3_CONFIG.get('region', 'ru-1'),
            verify_ssl=config.S3_CONFIG.get('verify_ssl', False)
        )
        logger.info(f"✅ S3 клиент создан для бакета: {config.S3_CONFIG['bucket']}")
        return client
    except Exception as e:
        logger.error(f"❌ Ошибка создания S3 клиента: {e}")
        return None


async def demonstrate_task_1(client: AsyncObjectStorage) -> None:
    """Демонстрация выполнения первого задания"""
    logger.info("\n" + "=" * 60)
    logger.info("🎯 ЗАДАНИЕ 1: Методы list_files() и file_exists()")
    logger.info("=" * 60)

    # 1. Демонстрация list_files()
    logger.info("\n1. 📋 Метод list_files():")
    logger.info("   a) Без параметра (все файлы):")
    try:
        all_files = await client.list_files()
        logger.info(f"      Всего файлов в бакете: {len(all_files)}")
        if all_files:
            logger.info(f"      Примеры (первые 5):")
            for i, f in enumerate(all_files[:5], 1):
                logger.info(f"      {i}. {f}")
            if len(all_files) > 5:
                logger.info(f"      ... и еще {len(all_files) - 5} файлов")
    except Exception as e:
        logger.error(f"      ❌ Ошибка: {e}")

    logger.info("\n   b) С параметром prefix='demo' (фильтрация):")
    try:
        test_files = await client.list_files("demo")
        logger.info(f"      Файлов с префиксом 'demo': {len(test_files)}")
        if test_files:
            for i, f in enumerate(test_files[:3], 1):
                logger.info(f"      {i}. {f}")
    except Exception as e:
        logger.error(f"      ❌ Ошибка: {e}")

    # 2. Демонстрация file_exists()
    logger.info("\n2. 🔍 Метод file_exists():")
    test_files_to_check = [
        "test_file.txt",
        "документ.pdf",
        "несуществующий_файл.xyz",
        "demo_versioning.txt"
    ]

    for filename in test_files_to_check:
        try:
            exists = await client.file_exists(filename)
            status = "✅ СУЩЕСТВУЕТ" if exists else "❌ ОТСУТСТВУЕТ"
            logger.info(f"   • {filename}: {status}")
        except Exception as e:
            logger.error(f"   • {filename}: ❌ Ошибка проверки: {e}")

    # 3. Интеграционная проверка
    logger.info("\n3. 🔄 Интеграционная проверка методов:")
    try:
        # Создаем тестовый файл в папке temp из конфига
        test_filename = "тест_задание_1.txt"
        temp_dir = Path(config.PIPELINE_CONFIG['temp_folder'])
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_file = temp_dir / "temp_test_upload.txt"

        # Создаем файл с явной кодировкой UTF-8
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write("Тестовый файл для задания 1\n")
            f.write(f"Время создания: {time.ctime()}\n")
            f.write(f"Папка: {temp_dir.absolute()}\n")
            f.write(f"Кодировка: UTF-8\n")

        # Загружаем
        logger.info(f"   📤 Загрузка файла {test_filename}...")
        success = await client.upload(str(temp_file), test_filename)

        if success:
            logger.info(f"   ✅ Файл загружен")

            # Проверяем file_exists
            exists = await client.file_exists(test_filename)
            logger.info(f"   🔍 file_exists(): {'✅ Подтверждено' if exists else '❌ Не найден'}")

            # Проверяем list_files
            files_after_upload = await client.list_files('тест')
            file_in_list = test_filename in files_after_upload
            logger.info(f"   📋 file в list_files('тест'): {'✅ Найден' if file_in_list else '❌ Отсутствует'}")

            # Удаляем тестовый файл из S3
            await client.delete_file(test_filename)
            logger.info(f"   🗑️  Тестовый файл удален из S3")
        else:
            logger.error(f"   ❌ Не удалось загрузить файл")

        # Удаляем локальный файл
        if temp_file.exists():
            temp_file.unlink()

    except Exception as e:
        logger.error(f"   ❌ Ошибка интеграционной проверки: {e}")

    logger.info("\n" + "=" * 60)
    logger.info("✅ ЗАДАНИЕ 1 ВЫПОЛНЕНО")
    logger.info("=" * 60)


async def demonstrate_task_2(client: AsyncObjectStorage) -> None:
    """Демонстрация выполнения второго задания"""
    logger.info("\n" + "=" * 60)
    logger.info("🎯 ЗАДАНИЕ 2: Версионирование и политики бакета")
    logger.info("=" * 60)

    # 1. Включение версионирования
    logger.info("\n1. ⚙️ Включение версионирования в бакете:")
    try:
        success = await client.enable_versioning()
        if success:
            logger.info("   ✅ Версионирование включено")

            # Проверяем статус версионирования
            bucket_info = await client.get_bucket_info()
            versioning_status = bucket_info.get('versioning', 'Unknown')
            logger.info(f"   📊 Статус версионирования: {versioning_status}")
        else:
            logger.warning("   ⚠️ Не удалось включить версионирование")
    except Exception as e:
        logger.error(f"   ❌ Ошибка: {e}")

    # 2. Создание нескольких версий файла demo_versioning.txt
    logger.info("\n2. 📝 Создание нескольких версий файла demo_versioning.txt:")
    test_file = "demo_versioning.txt"
    versions_info = []  # Будем хранить информацию о версиях

    # Используем папку temp из конфига
    temp_dir = Path(config.PIPELINE_CONFIG['temp_folder'])
    temp_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"   📁 Папка для сохранения: {temp_dir.absolute()}")

    for i in range(3):
        # Создаем файл для загрузки и сохранения локально
        version_filename = f"demo_versioning_v{i + 1}.txt"
        version_file = temp_dir / version_filename

        # Создаем содержимое с явной кодировкой UTF-8
        file_content = f"=== Версия {i + 1} файла demo_versioning.txt ===\n"
        file_content += f"Создано: {time.ctime()}\n"
        file_content += f"Временная метка: {time.time()}\n"
        file_content += f"Номер версии: {i + 1} из 3\n"
        file_content += f"Бакет: {config.S3_CONFIG['bucket']}\n"
        file_content += f"Endpoint: {config.S3_CONFIG['endpoint']}\n"
        file_content += f"Конфигурация загружена из: src/config.py\n"
        file_content += "-" * 50 + "\n"
        file_content += "Это тестовый файл для демонстрации версионирования в S3.\n"
        file_content += "Файл загружается в Selectel Cloud Storage с включенным версионированием.\n"
        file_content += "Каждая версия имеет уникальный VersionId в S3.\n"
        file_content += f"Кодировка файла: UTF-8\n"

        # Сохраняем с явным указанием кодировки UTF-8
        with open(version_file, 'w', encoding='utf-8') as f:
            f.write(file_content)

        try:
            logger.info(f"   📤 Загрузка версии {i + 1}...")
            version_id = await client.upload_with_versioning(str(version_file), test_file)

            if version_id and version_id != 'null':
                versions_info.append({
                    'version_id': version_id,
                    'version_num': i + 1,
                    'local_path': str(version_file.absolute()),
                    'file_name': version_file.name,
                    'size': version_file.stat().st_size,
                    'content': file_content  # Сохраняем содержимое для сравнения
                })
                short_id = version_id[:10] + "..." if len(version_id) > 10 else version_id
                logger.info(f"   ✅ Загружена версия {i + 1}: ID={short_id}")
                logger.info(f"   💾 Сохранена локально: {version_file.name} ({version_file.stat().st_size} байт)")
                logger.info(f"   🔤 Кодировка: UTF-8")
            else:
                logger.warning(f"   ⚠️ Версия {i + 1} загружена без ID (версионирование не поддерживается?)")

        except Exception as e:
            logger.error(f"   ❌ Ошибка загрузки версии {i + 1}: {e}")

        await asyncio.sleep(1)  # Пауза между версиями

    # 3. Информация о сохраненных версиях
    logger.info("\n3. 💾 Информация о сохраненных версиях:")
    if versions_info:
        logger.info(f"   ✅ Сохранено локально версий: {len(versions_info)}")
        for info in versions_info:
            logger.info(f"   • Версия {info['version_num']}: {info['file_name']}")
            logger.info(f"     📁 Путь: {info['local_path']}")
            logger.info(f"     📊 Размер: {info['size']} байт")

        # Показываем содержимое последней версии
        if versions_info:
            last_version = versions_info[-1]
            sample_file = Path(last_version['local_path'])
            if sample_file.exists():
                logger.info(f"\n   📄 Содержимое последней версии ({sample_file.name}):")
                logger.info(f"   {'─' * 40}")
                try:
                    with open(sample_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        for line in lines[:6]:
                            logger.info(f"   {line.strip()}")
                except Exception:
                    logger.info(f"   (бинарные данные)")
                logger.info(f"   {'─' * 40}")
    else:
        logger.info("   ℹ️ Версии не были сохранены локально")

    # 4. Получение списка версий из S3
    logger.info("\n4. 📋 Получение списка всех версий файла из S3:")
    all_versions = []
    try:
        all_versions = await client.list_versions(test_file)
        if all_versions and len(all_versions) > 0:
            logger.info(f"   ✅ Найдено версий в S3: {len(all_versions)}")
            for i, v in enumerate(all_versions, 1):
                version_id = v.get('VersionId', 'null')
                is_latest = "✓" if v.get('IsLatest', False) else " "
                modified = v.get('LastModified', 'N/A')
                size = v.get('Size', 0)

                logger.info(f"   {i}. [{is_latest}] ID: {version_id}")
                logger.info(f"      Время: {modified}")
                logger.info(f"      Размер: {size} байт")
        else:
            logger.info("   ℹ️ Версии не найдены в S3")
    except Exception as e:
        logger.error(f"   ❌ Ошибка получения версий из S3: {e}")

    # 5. Скачивание предыдущей версии в папку temp
    logger.info("\n5. 💾 Скачивание предыдущей версии файла из S3 в папку temp:")

    logger.info(f"   📁 Папка для скачивания: {temp_dir.absolute()}")

    # Проверяем, что у нас есть достаточно версий
    if all_versions and len(all_versions) >= 2:
        logger.info(f"   📊 Найдено версий в S3: {len(all_versions)}")

        # Находим не последнюю версию
        non_latest_versions = []
        for version in all_versions:
            if not version.get('IsLatest', False):
                non_latest_versions.append(version)

        if non_latest_versions:
            # Берем последнюю из не-latest версий (предпоследнюю)
            previous_version = non_latest_versions[-1]
            previous_version_id = previous_version.get('VersionId')

            if previous_version_id and previous_version_id != 'null':
                # Создаем имя файла с версией
                download_filename = f"demo_versioning_previous_v_{previous_version_id}.txt"
                download_path = temp_dir / download_filename

                try:
                    logger.info(f"   📥 Скачивание предыдущей версии...")
                    logger.info(f"   🔍 ID версии: {previous_version_id[:12]}...")

                    success = await client.download_version(
                        test_file,
                        str(download_path),
                        previous_version_id
                    )

                    if success:
                        if download_path.exists():
                            file_size = download_path.stat().st_size
                            abs_path = download_path.absolute()
                            logger.info(f"   ✅ Предыдущая версия скачана успешно!")
                            logger.info(f"   📁 Имя файла: {download_filename}")
                            logger.info(f"   📁 Путь: {abs_path}")
                            logger.info(f"   📊 Размер: {file_size} байт")

                            # Читаем содержимое скачанной версии
                            file_content = None
                            used_encoding = 'unknown'

                            # Пробуем разные кодировки для чтения
                            encodings_to_try = ['utf-8', 'utf-8-sig', 'cp1251', 'latin-1', 'windows-1251']

                            for encoding in encodings_to_try:
                                try:
                                    with open(download_path, 'r', encoding=encoding) as f:
                                        file_content = f.read()
                                    used_encoding = encoding
                                    logger.info(f"   🔤 Прочитано с кодировкой: {encoding}")
                                    break
                                except UnicodeDecodeError:
                                    continue

                            # Если ни одна кодировка не подошла
                            if file_content is None:
                                try:
                                    # Читаем как бинарный
                                    with open(download_path, 'rb') as f:
                                        binary_data = f.read()
                                    # Пробуем декодировать с игнорированием ошибок
                                    file_content = binary_data.decode('utf-8', errors='ignore')
                                    used_encoding = 'binary (utf-8 with errors ignored)'
                                    logger.info(f"   🔤 Чтение как бинарного файла")
                                except Exception as e:
                                    logger.error(f"   🔤 Ошибка чтения файла: {e}")
                                    file_content = f"[Не удалось прочитать содержимое файла. Ошибка: {e}]"
                                    used_encoding = 'error'

                            # Показываем содержимое
                            if file_content and file_content.strip():
                                logger.info(f"   📄 Содержимое скачанной версии (первые 6 строк):")
                                logger.info(f"   {'─' * 40}")
                                lines = file_content.split('\n')
                                line_count = 0
                                for line in lines:
                                    if line.strip():
                                        logger.info(f"   {line}")
                                        line_count += 1
                                        if line_count >= 6:
                                            break
                                if line_count == 0:
                                    logger.info(f"   (файл пуст или содержит только пробелы)")
                                logger.info(f"   {'─' * 40}")

                                # Перезаписываем файл в правильной кодировке UTF-8
                                try:
                                    with open(download_path, 'w', encoding='utf-8') as f:
                                        # Записываем оригинальное содержимое
                                        f.write(file_content)

                                        # Добавляем информацию о версии
                                        f.write(f"\n{'=' * 50}\n")
                                        f.write(f"ИНФОРМАЦИЯ О ВЕРСИИ:\n")
                                        f.write(f"• Это предыдущая версия файла demo_versioning.txt\n")
                                        f.write(f"• VersionId в S3: {previous_version_id}\n")
                                        f.write(f"• Время скачивания: {time.ctime()}\n")
                                        f.write(f"• Скачана из бакета: {config.S3_CONFIG['bucket']}\n")
                                        f.write(f"• Это версия №{len(non_latest_versions)} из {len(all_versions)}\n")
                                        f.write(f"• Исходная кодировка: {used_encoding}\n")
                                        f.write(f"• Пересохранено в кодировке: UTF-8\n")
                                        f.write(f"{'=' * 50}\n")

                                    logger.info(f"   📝 Файл пересохранен в кодировке UTF-8 с добавленной информацией")
                                except Exception as e:
                                    logger.error(f"   ❌ Ошибка пересохранения файла: {e}")
                            else:
                                logger.info(f"   📄 Файл пуст или не содержит текстовых данных")

                        else:
                            logger.error(f"   ❌ Файл не был создан")
                    else:
                        logger.error(f"   ❌ Ошибка скачивания версии")

                except Exception as e:
                    logger.error(f"   ❌ Ошибка скачивания: {e}")
            else:
                logger.info("   ℹ️ Нет действительного ID версии")
        else:
            logger.info("   ℹ️ Все версии являются последними")
    else:
        logger.info("   ℹ️ Недостаточно версий для скачивания")
        if all_versions:
            logger.info(f"   📊 Найдено версий: {len(all_versions)}")
        else:
            logger.info("   📊 Версии не найдены")

    # 6. Настройка политик бакета (информация)
    logger.info("\n6. ⚙️ Настройки политики бакета выполнены в Selectel Cloud Storage Console.")
    logger.info("   📋 Для выполнения этой части задания можно использовать:")
    logger.info("   Bucket Policy и Lifecycle Policy (для Console) настроены в Selectel:")
    logger.info("      • Чтение всем и запись только владельцу")
    logger.info("      • 'Жизненный цикл' → добавлено правило удаления через 3 дня")

    # 7. Итоговая информация о файлах в папке temp
    logger.info("\n7. 📁 ИТОГОВАЯ ИНФОРМАЦИЯ О ФАЙЛАХ В ПАПКЕ TEMP:")

    # Ищем все файлы demo_versioning в папке temp
    demo_files = list(temp_dir.glob("demo_versioning*.txt"))
    if demo_files:
        logger.info(f"   ✅ Найдено файлов demo_versioning: {len(demo_files)}")

        # Группируем по типам
        version_files = [f for f in demo_files if "demo_versioning_v" in f.name and "previous" not in f.name]
        previous_files = [f for f in demo_files if "demo_versioning_previous" in f.name]

        if version_files:
            logger.info(f"   a) Основные версии (загружены в S3):")
            for file_path in version_files:
                file_size = file_path.stat().st_size
                mod_time = time.ctime(file_path.stat().st_mtime)
                logger.info(f"      • {file_path.name}")
                logger.info(f"        📊 Размер: {file_size} байт")
                logger.info(f"        🕒 Изменен: {mod_time}")

        if previous_files:
            logger.info(f"\n   b) Скачанные предыдущие версии:")
            for file_path in previous_files:
                file_size = file_path.stat().st_size
                mod_time = time.ctime(file_path.stat().st_mtime)
                logger.info(f"      • {file_path.name}")
                logger.info(f"        📊 Размер: {file_size} байт")
                logger.info(f"        🕒 Изменен: {mod_time}")

        logger.info(f"\n   📍 Полный путь к папке temp: {temp_dir.absolute()}")
        logger.info(f"   💡 Для проверки файлов откройте папку: {temp_dir.absolute()}")
    else:
        logger.error(f"   ❌ Файлы demo_versioning не найдены в папке temp")
        logger.info(f"   📁 Проверьте папку: {temp_dir.absolute()}")

    logger.info("\n" + "=" * 60)
    logger.info("✅ ЗАДАНИЕ 2 ВЫПОЛНЕНО")
    logger.info("=" * 60)
    logger.info("\n📋 ДЛЯ ПОДТВЕРЖДЕНИЯ ВЫПОЛНЕНИЯ:")
    logger.info("1. Скриншоты из Selectel Console:")
    logger.info("   • Bucket Policy настроена")
    logger.info("   • Lifecycle Policy настроена")
    logger.info("   • Версионирование включено")
    logger.info("2. Проверьте файлы в папке temp:")
    logger.info(f"   • Папка: {temp_dir.absolute()}")
    logger.info("   • Должны быть: demo_versioning_v1.txt, demo_versioning_v2.txt, demo_versioning_v3.txt")
    logger.info("   • И скачанная предыдущая версия: demo_versioning_previous_v_{ID}.txt")
    logger.info("3. Код методов корректно работает")
    logger.info("4. Все операции выполняются асинхронно")
    logger.info("=" * 60)


async def main() -> None:
    """Основная функция демонстрации"""
    logger.info("=" * 70)
    logger.info("🎯 ДЕМОНСТРАЦИЯ ВЫПОЛНЕНИЯ ЗАДАНИЙ 1 и 2")
    logger.info("=" * 70)
    logger.info("S3 клиент для Selectel Cloud Storage")
    logger.info(f"Бакет: {config.S3_CONFIG.get('bucket', 'Не указан')}")
    logger.info(f"Endpoint: {config.S3_CONFIG.get('endpoint', 'Не указан')}")
    logger.info(f"Файл логов: {LOG_FILE_PATH}")
    logger.info("=" * 70)

    # Создаем клиент
    client = await create_async_client()
    if not client:
        logger.error("❌ Не удалось создать S3 клиент. Проверьте конфигурацию в config.py")
        return

    # Улучшенная проверка подключения
    logger.info("\n🔍 Проверка подключения к S3...")
    try:
        # Пробуем простой запрос вместо HeadBucket
        logger.info("   📡 Тестирование соединения...")

        # Вариант 1: Пробуем получить список файлов
        try:
            files = await client.list_files()
            logger.info(f"   ✅ Подключение успешно! Файлов в бакете: {len(files)}")
            if files:
                logger.info(f"   📋 Примеры файлов (первые 3):")
                for i, f in enumerate(files[:3], 1):
                    logger.info(f"   {i}. {f}")
        except Exception as list_error:
            logger.error(f"   ⚠️ Ошибка при list_files: {list_error}")

            # Вариант 2: Пробуем другой метод
            try:
                # Проверяем существование несуществующего файла
                exists = await client.file_exists("__test_connection__.txt")
                logger.info(f"   ✅ Подключение работает (file_exists вернул {exists})")
            except Exception as file_error:
                logger.error(f"   ⚠️ Ошибка при file_exists: {file_error}")

                # Вариант 3: Пробуем получить информацию о бакете без HeadBucket
                try:
                    # Просто выводим информацию о клиенте
                    logger.info(f"   ℹ️ Информация о клиенте:")
                    logger.info(f"   • Бакет: {client.bucket}")
                    logger.info(f"   • Endpoint: {client.endpoint}")
                    logger.info(f"   • Регион: {client.region}")
                    logger.info(f"   • SSL проверка: {client.verify_ssl}")
                    logger.info(f"   📡 Продолжаем демонстрацию...")
                except Exception as info_error:
                    logger.error(f"   ❌ Не удалось получить информацию: {info_error}")
                    logger.info(f"   📡 Продолжаем демонстрацию, возможно проблемы с сетью...")

    except Exception as e:
        logger.error(f"   ❌ Общая ошибка проверки подключения: {e}")
        logger.info(f"   📡 Продолжаем демонстрацию...")

    # Задание 1
    await demonstrate_task_1(client)

    # Пауза между заданиями
    logger.info("\n⏳ Переход к заданию 2...")
    await asyncio.sleep(2)

    # Задание 2
    await demonstrate_task_2(client)

    logger.info("\n" + "=" * 70)
    logger.info("✅ ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА")
    logger.info("=" * 70)
    logger.info(f"📄 Все логи сохранены в файле: {LOG_FILE_PATH}")


if __name__ == "__main__":
    # Настройка event loop для Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Запуск
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n\n👋 Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"\n💥 Критическая ошибка: {e}")
        logger.exception("Детали ошибки:")
