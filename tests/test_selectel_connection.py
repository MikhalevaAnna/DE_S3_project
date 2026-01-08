import asyncio
from config import config
from src.async_s3_client import AsyncObjectStorage


async def test_connection():
    client = AsyncObjectStorage(
        key_id=config.S3_CONFIG['access_key'],
        secret=config.S3_CONFIG['secret_key'],
        endpoint=config.S3_CONFIG['endpoint'],
        container=config.S3_CONFIG['bucket'],
        region=config.S3_CONFIG.get('region', 'ru-1'),
        verify_ssl=config.S3_CONFIG.get('verify_ssl', False)
    )

    print(f"Bucket: {config.S3_CONFIG['bucket']}")
    print("Тестирование подключения...")

    try:
        files = await client.list_files()  # ← ДОБАВЬТЕ await
        print(f"✓ Успешно! Файлов в бакете: {len(files)}")
    except Exception as e:
        print(f"✗ Ошибка: {e}")
        print("\nПроверьте:")
        print(f"1. endpoint: {config.S3_CONFIG['endpoint']}")
        print(f"2. bucket: {config.S3_CONFIG['bucket']}")
        print(f"3. access_key: {'*' * len(config.S3_CONFIG['access_key'])}")


# Запускаем асинхронную функцию
if __name__ == "__main__":
    asyncio.run(test_connection())  # ← ЗАПУСКАЕМ АСИНХРОННО