import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # Для считывания файла .env с ключами

# Базовые пути проекта
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

# Конфигурация S3 со значениями по умолчанию
S3_CONFIG = {
    'endpoint': os.getenv('S3_ENDPOINT', 'http://localhost:9000'),  # дефолт для локального MinIO
    'access_key': os.getenv('S3_ACCESS_KEY', ''),
    'secret_key': os.getenv('S3_SECRET_KEY', ''),
    'bucket': os.getenv('S3_BUCKET', 'de-practice'),
    'region': 'ru-1',  # Регион для Selectel
    'verify_ssl': False  # Для Selectel часто нужно отключать SSL проверку
}


# Конфигурация пайплайна
PIPELINE_CONFIG = {
    'watch_folder': str(DATA_DIR / "incoming"),
    'processed_folder': str(DATA_DIR / "processed"),
    'temp_folder': str(DATA_DIR / "temp"),
    'log_folder': str(DATA_DIR / "logs"),
    's3_processed_folder': 'processed',
    's3_logs_folder': 'logs',
    'filter_threshold': 55000,  # Порог для фильтрации salary
    'max_threshold': 1000000, # Максимальный порог, какой может быть зарплата
    # Настройки обработки
    'supported_formats': ['.csv', '.json', '.xlsx', '.xls', '.parquet', '.txt'],
    'check_interval': 5,  # Интервал проверки файлов (секунды)

}



# Создание директорий при импорте
for folder in [
    DATA_DIR / "incoming",
    DATA_DIR / "processed",
    DATA_DIR / "temp",
    DATA_DIR / "logs"
]:
    folder.mkdir(parents=True, exist_ok=True)

