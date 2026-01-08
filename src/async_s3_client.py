"""
Асинхронная обертка над boto3 для работы с S3 хранилищами.
Для Selectel Cloud Storage.
"""
import asyncio
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pathlib import Path
import logging
from typing import List, Dict, Optional
from functools import partial


# Подавляем предупреждения для boto3
import warnings

warnings.filterwarnings('ignore', category=Warning)
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AsyncObjectStorage:
    """
    Асинхронная обертка над boto3 для работы с S3.
    """

    def __init__(
            self,
            *,
            key_id: str,
            secret: str,
            endpoint: str,
            container: str,
            region: str = "ru-1",
            verify_ssl: bool = False
    ):
        """
        Инициализация асинхронного S3 клиента.

        Args:
            key_id: Access Key ID
            secret: Secret Access Key
            endpoint: URL S3-хранилища
            container: Имя бакета
            region: Регион S3 хранилища
            verify_ssl: Проверять SSL сертификаты (для Selectel часто нужно False)
        """
        self.bucket = container
        self.endpoint = endpoint
        self.region = region
        self.verify_ssl = verify_ssl

        # Создаем сессию boto3
        session = boto3.session.Session()

        # Конфигурация для Selectel
        config = Config(
            connect_timeout=30,
            read_timeout=60,
            retries={'max_attempts': 3},
            s3={'addressing_style': 'virtual'},
            signature_version='s3v4'
        )

        # Создаем клиент с подавлением предупреждений
        self.s3_client = session.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            region_name=region,
            config=config,
            verify=verify_ssl
        )

        # Настраиваем логгер для boto3
        logging.getLogger('boto3').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Инициализирован клиент для {endpoint}/{container}")
        self.logger.info(f"SSL проверка: {verify_ssl}")

    async def _run_in_executor(self, func, *args, **kwargs):
        """Запускает синхронную функцию в executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    async def upload(self, file_path: str, object_name: str) -> bool:
        """Асинхронная загрузка файла в S3."""
        try:
            file_ref = Path(file_path)
            if not file_ref.exists():
                self.logger.error(f"Файл не существует: {file_path}")
                return False

            self.logger.info(f"Начало загрузки: {object_name} ({file_path})")

            await self._run_in_executor(
                self.s3_client.upload_file,
                file_path,
                self.bucket,
                object_name
            )

            self.logger.info(f"Загружено: {object_name}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            self.logger.error(f"Ошибка загрузки {object_name}: {error_code}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка загрузки {object_name}: {e}")
            return False

    async def upload_with_versioning(self, file_path: str, object_name: str) -> Optional[str]:
        """Асинхронная загрузка файла с версионированием."""
        try:
            # Сначала загружаем файл
            success = await self.upload(file_path, object_name)
            if not success:
                return None

            # Получаем информацию о загруженном объекте
            try:
                response = await self._run_in_executor(
                    self.s3_client.head_object,
                    Bucket=self.bucket,
                    Key=object_name
                )
                version_id = response.get('VersionId', 'null')
                self.logger.info(f"Загружено с версионированием: {object_name}, VersionId: {version_id}")
                return version_id
            except ClientError:
                # Если версионирование не поддерживается
                return 'null'

        except Exception as e:
            self.logger.error(f"Ошибка загрузки с версионированием: {e}")
            return None

    async def download(self, object_name: str, save_path: str) -> bool:
        """Асинхронное скачивание файла из S3."""
        try:
            save_dir = Path(save_path).parent
            save_dir.mkdir(parents=True, exist_ok=True)

            self.logger.info(f"Начало скачивания: {object_name} -> {save_path}")

            await self._run_in_executor(
                self.s3_client.download_file,
                self.bucket,
                object_name,
                save_path
            )

            self.logger.info(f"Скачано: {object_name} -> {save_path}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                self.logger.error(f"Файл не найден: {object_name}")
            else:
                self.logger.error(f"Ошибка скачивания {object_name}: {error_code}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка скачивания {object_name}: {e}")
            return False

    async def download_version(self, object_name: str, save_path: str, version_id: str) -> bool:
        """Асинхронное скачивание конкретной версии файла."""
        try:
            save_dir = Path(save_path).parent
            save_dir.mkdir(parents=True, exist_ok=True)

            self.logger.info(f"Начало скачивания версии {version_id}: {object_name}")

            # Для скачивания конкретной версии используем get_object
            response = await self._run_in_executor(
                self.s3_client.get_object,
                Bucket=self.bucket,
                Key=object_name,
                VersionId=version_id
            )

            # Читаем данные
            body = response['Body'].read()

            # Сохраняем файл
            with open(save_path, 'wb') as f:
                f.write(body)

            self.logger.info(f"Скачана версия {version_id} файла {object_name} -> {save_path}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchVersion':
                self.logger.error(f"Версия {version_id} не существует для файла {object_name}")
            elif error_code == 'NoSuchKey':
                self.logger.error(f"Файл не найден: {object_name}")
            else:
                self.logger.error(f"Ошибка скачивания версии {object_name}: {error_code}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка скачивания версии {object_name}: {e}")
            return False

    async def list_files(self, prefix: str = "") -> List[str]:
        """Асинхронное получение списка файлов в бакете."""
        try:
            response = await self._run_in_executor(
                self.s3_client.list_objects_v2,
                Bucket=self.bucket,
                Prefix=prefix
            )

            if 'Contents' not in response:
                self.logger.info(f"Файлы с префиксом '{prefix}' не найдены")
                return []

            files = [obj['Key'] for obj in response['Contents']]
            if prefix == '':
                self.logger.info(f"Получено файлов: {len(files)}")
            else:
                self.logger.info(f"Получено файлов с префиксом '{prefix}': {len(files)}")
            return files

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                self.logger.error(f"Бакет {self.bucket} не существует")
            else:
                self.logger.error(f"Ошибка получения списка файлов: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка получения списка файлов: {e}")
            return []

    async def file_exists(self, object_name: str) -> bool:
        """Асинхронная проверка существования файла в S3."""
        try:
            self.logger.debug(f"Проверка существования файла: {object_name}")

            await self._run_in_executor(
                self.s3_client.head_object,
                Bucket=self.bucket,
                Key=object_name
            )
            self.logger.debug(f"Файл существует: {object_name}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.debug(f"Файл не существует: {object_name}")
                return False
            else:
                self.logger.error(f"Ошибка проверки существования файла {object_name}: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка проверки существования файла {object_name}: {e}")
            return False

    async def enable_versioning(self) -> bool:
        """Асинхронное включение версионирования для бакета."""
        try:
            self.logger.info(f"Включение версионирования для бакета: {self.bucket}")

            await self._run_in_executor(
                self.s3_client.put_bucket_versioning,
                Bucket=self.bucket,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            self.logger.info(f"Версионирование включено для бакета: {self.bucket}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                self.logger.error(f"Нет прав для включения версионирования в бакете {self.bucket}")
            elif error_code == 'NotImplemented':
                self.logger.warning("Версионирование не поддерживается хранилищем")
            else:
                self.logger.error(f"Ошибка включения версионирования: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка включения версионирования: {e}")
            return False

    async def list_versions(self, object_name: Optional[str] = None) -> List[Dict]:
        """Асинхронное получение списка версий объектов."""
        try:
            kwargs = {'Bucket': self.bucket}
            if object_name:
                kwargs['Prefix'] = object_name
                self.logger.info(f"Получение версий файла: {object_name}")
            else:
                self.logger.info(f"Получение всех версий в бакете")

            response = await self._run_in_executor(
                self.s3_client.list_object_versions,
                **kwargs
            )

            versions = []
            if 'Versions' in response:
                for v in response['Versions']:
                    versions.append({
                        'Key': v['Key'],
                        'VersionId': v['VersionId'],
                        'LastModified': v['LastModified'],
                        'IsLatest': v.get('IsLatest', False),
                        'Size': v.get('Size', 0)
                    })

            self.logger.info(f"Получено версий: {len(versions)}")
            return versions

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                self.logger.error(f"Нет прав для получения списка версий")
            else:
                self.logger.error(f"Ошибка получения версий: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка получения версий: {e}")
            return []

    async def delete_file(self, object_name: str) -> bool:
        """Асинхронное удаление файла из S3."""
        try:
            self.logger.info(f"Удаление файла: {object_name}")

            await self._run_in_executor(
                self.s3_client.delete_object,
                Bucket=self.bucket,
                Key=object_name
            )
            self.logger.info(f"Удалено: {object_name}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                self.logger.warning(f"Файл для удаления не найден: {object_name}")
                return True  # Файл уже удален
            else:
                self.logger.error(f"Ошибка удаления {object_name}: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка удаления {object_name}: {e}")
            return False

    async def get_bucket_info(self) -> Dict:
        """Получение информации о бакете."""
        try:
            # Проверяем существование бакета
            try:
                await self._run_in_executor(
                    self.s3_client.head_bucket,
                    Bucket=self.bucket
                )
                bucket_exists = True
                self.logger.debug(f"Бакет существует: {self.bucket}")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    bucket_exists = False
                    self.logger.warning(f"Бакет не существует: {self.bucket}")
                else:
                    self.logger.warning(f"Не удалось проверить бакет {self.bucket}: {e}")
                    bucket_exists = True  # Предполагаем, что бакет существует

            # Получаем информацию о версионировании
            try:
                response = await self._run_in_executor(
                    self.s3_client.get_bucket_versioning,
                    Bucket=self.bucket
                )
                versioning_status = response.get('Status', 'Disabled')
                self.logger.debug(f"Статус версионирования: {versioning_status}")
            except ClientError:
                versioning_status = 'Unknown'
                self.logger.debug(f"Не удалось получить статус версионирования")
            except Exception as e:
                self.logger.warning(f"Не удалось получить статус версионирования: {e}")
                versioning_status = 'Unknown'

            return {
                'name': self.bucket,
                'exists': bucket_exists,
                'versioning': versioning_status,
                'endpoint': self.endpoint,
                'region': self.region,
                'ssl_verify': self.verify_ssl
            }
        except Exception as e:
            self.logger.error(f"Ошибка получения информации о бакете: {e}")
            return {
                'name': self.bucket,
                'exists': False,
                'versioning': 'Unknown',
                'endpoint': self.endpoint,
                'region': self.region,
                'ssl_verify': self.verify_ssl
            }