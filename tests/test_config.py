from config import config

print("=" * 50)
print("ПРОВЕРКА КОНФИГУРАЦИИ")
print("=" * 50)

print("\n1. Проверка загрузки из .env:")
for key, value in config.S3_CONFIG.items():
    # Скрываем секретные ключи
    if 'secret' in key or 'key' in key:
        masked_value = f"{value[:5]}..." if value else "НЕ НАЙДЕНО"
        print(f"   {key}: {masked_value}")
    else:
        print(f"   {key}: {value}")

print("\n2. Проверка путей:")
print(f"   BASE_DIR: {config.BASE_DIR}")
print(f"   DATA_DIR: {config.DATA_DIR}")
print(f"   watch_folder: {config.PIPELINE_CONFIG['watch_folder']}")

print("\n3. Проверка существования .env переменных:")
import os
env_vars = ['S3_ENDPOINT', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET']
for var in env_vars:
    value = os.getenv(var)
    status = "✓ ЗАГРУЖЕНО" if value else "✗ ОТСУТСТВУЕТ"
    if 'SECRET' in var or 'KEY' in var and value:
        print(f"   {var}: {status} (длина: {len(value)})")
    else:
        print(f"   {var}: {status} ({value})")

print("\n" + "=" * 50)