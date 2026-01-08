"""
Асинхронный пайплайн обработки данных для задания 3.
С фильтрацией по зарплате.
"""
import asyncio
import pandas as pd
from pathlib import Path
import logging
import json
import time
from datetime import datetime
from typing import Dict, Optional, Any, List


class DataPipeline:
    """
    Асинхронный пайплайн для обработки файлов данных.
    Фильтрация по зарплате: данные попадают в отфильтрованный файл,
    если зарплата больше или равна заданному условию.
    """

    def __init__(self, s3_client, config: Dict[str, Any]):
        """
        Инициализация пайплайна.

        Args:
            s3_client: Асинхронный S3 клиент
            config: Конфигурация пайплайна
        """
        self.s3_client = s3_client
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

        # Создаем папки если не существуют
        self.watch_folder = Path(config['watch_folder'])
        self.temp_folder = Path(config['temp_folder'])
        self.processed_folder = Path(config['processed_folder'])
        self.log_folder = Path(config['log_folder'])
        self.filter = int(config['filter_threshold'])
        self.max_threshold = int(config['max_threshold'])

        for folder in [self.watch_folder, self.temp_folder,
                       self.processed_folder, self.log_folder]:
            folder.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Пайплайн инициализирован")
        self.logger.info(f"Фильтрация: зарплата > {self.filter}")
        self.logger.info(f"Папка наблюдения: {self.watch_folder}")
        self.logger.info(f"Папка обработки: {self.processed_folder}")

    async def process_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Обработка одного файла через пайплайн.

        Returns:
            Dict с результатами обработки
        """
        result = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'start_time': datetime.now().isoformat(),
            'success': False,
            'error': None,
            'records_processed': 0,
            'records_filtered': 0,
            'filtered_by_salary': 0,  # Новое поле: отфильтровано по зарплате
            's3_path': None,
            'version_id': None
        }

        try:
            # Шаг 1: Проверка файла
            self.logger.info(f"📁 Начало обработки файла: {file_path.name}")

            if not file_path.exists():
                result['error'] = f"Файл не существует: {file_path}"
                self.logger.error(result['error'])
                return result

            file_size = file_path.stat().st_size
            self.logger.info(f"   Размер файла: {file_size} байт")

            # Шаг 2: Чтение данных в зависимости от формата
            df = await self._read_data_file(file_path)
            if df is None:
                result['error'] = f"Не удалось прочитать файл: {file_path}"
                self.logger.error(result['error'])
                return result

            result['records_processed'] = len(df)
            self.logger.info(f"   Прочитано записей: {len(df)}")

            # Шаг 3: Обработка данных с фильтрацией по зарплате
            processed_df, salary_stats = await self._process_data_with_salary_filter(df)
            result['records_filtered'] = len(processed_df)
            result['filtered_by_salary'] = salary_stats.get('filtered_count', 0)
            result['salary_stats'] = salary_stats

            if len(processed_df) == 0:
                self.logger.warning(f"   После фильтрации данных не осталось")
            else:
                self.logger.info(f"   После фильтрации осталось: {len(processed_df)} записей")
                self.logger.info(f"   Отфильтровано по зарплате: {salary_stats.get('filtered_count', 0)} записей")

            # Шаг 4: Сохранение во временный файл
            temp_file = await self._save_temp_file(processed_df, file_path, result)
            if temp_file is None:
                result['error'] = "Не удалось сохранить временный файл"
                self.logger.error(result['error'])
                return result

            # Шаг 5: Загрузка в S3
            s3_object_name = (f"processed/"
                              f"{datetime.now().strftime('%Y-%m-%d')}/"
                              f"salary_filtered_{file_path.stem}_{int(time.time())}.csv")

            self.logger.info(f"   📤 Загрузка в S3: {s3_object_name}")
            success = await self.s3_client.upload(str(temp_file), s3_object_name)

            if success:
                # Получаем версию файла
                try:
                    version_id = await self.s3_client.upload_with_versioning(str(temp_file), s3_object_name)
                    result['version_id'] = version_id
                except:
                    result['version_id'] = 'unknown'

                result['s3_path'] = s3_object_name
                result['success'] = True
                self.logger.info(f"   ✅ Файл загружен в S3: {s3_object_name}")

                # Шаг 6: Перемещение исходного файла
                await self._move_original_file(file_path)

            else:
                result['error'] = "Не удалось загрузить файл в S3"
                self.logger.error(result['error'])

            # Шаг 7: Удаление временного файла
            if temp_file.exists():
                temp_file.unlink()
                self.logger.info(f"   🗑️  Временный файл удален: {temp_file.name}")

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Ошибка обработки файла {file_path.name}: {e}")

        result['end_time'] = datetime.now().isoformat()
        return result

    async def _read_data_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Чтение файла данных в зависимости от формата.
        """
        try:
            ext = file_path.suffix.lower()

            if ext == '.csv':
                # Пробуем разные кодировки
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except:
                    try:
                        df = pd.read_csv(file_path, encoding='cp1251')
                    except:
                        df = pd.read_csv(file_path, encoding='utf-8', errors='replace')
            elif ext == '.json':
                df = pd.read_json(file_path)
            elif ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif ext == '.parquet':
                df = pd.read_parquet(file_path)
            else:
                # Пробуем как текстовый файл
                try:
                    df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
                except:
                    self.logger.error(f"Неподдерживаемый формат файла: {ext}")
                    return None

            self.logger.info(f"   Формат: {ext}, колонки: {list(df.columns)}")
            self.logger.info(f"   Размер: {df.shape[0]} строк, {df.shape[1]} столбцов")

            return df

        except Exception as e:
            self.logger.error(f"Ошибка чтения файла {file_path}: {e}")
            return None

    async def _process_data_with_salary_filter(self, df: pd.DataFrame) -> tuple[pd.DataFrame, Dict]:
        """
        Обработка и фильтрация данных по зарплате.

        Возвращает:
            - Отфильтрованный DataFrame
            - Статистику фильтрации
        """
        if df.empty:
            return df, {'filtered_count': 0, 'salary_columns': []}

        processed_df = df.copy()
        salary_stats = {
            'filtered_count': 0,
            'salary_columns': [],
            'original_count': len(df)
        }

        try:
            # Шаг 1: Поиск колонок с зарплатой
            salary_columns = self._find_salary_columns(processed_df)
            salary_stats['salary_columns'] = salary_columns

            if not salary_columns:
                self.logger.warning("   ⚠️ Колонки с зарплатой не найдены")
                self.logger.info("   Доступные колонки для фильтрации:")
                for col in processed_df.columns:
                    col_type = processed_df[col].dtype
                    self.logger.info(f"     - {col} ({col_type})")
                return processed_df, salary_stats

            self.logger.info(f"   Найдены колонки с зарплатой: {salary_columns}")

            # Шаг 2: Очистка данных
            initial_count = len(processed_df)

            # Удаление дубликатов
            processed_df = processed_df.drop_duplicates()
            dup_removed = initial_count - len(processed_df)
            if dup_removed > 0:
                self.logger.info(f"   Удалено дубликатов: {dup_removed}")

            # Шаг 3: Фильтрация по зарплате
            for salary_col in salary_columns:
                if salary_col in processed_df.columns:
                    # Конвертируем в числовой формат
                    processed_df[salary_col] = pd.to_numeric(processed_df[salary_col], errors='coerce')

                    # Статистика до фильтрации
                    salary_before = processed_df[salary_col].describe()
                    self.logger.info(f"   Статистика по {salary_col} до фильтрации:")
                    self.logger.info(f"     Мин: {salary_before.get('min', 'N/A'):.2f}")
                    self.logger.info(f"     Макс: {salary_before.get('max', 'N/A'):.2f}")
                    self.logger.info(f"     Среднее: {salary_before.get('mean', 'N/A'):.2f}")

                    # ФИЛЬТРАЦИЯ: зарплата > 100
                    mask = processed_df[salary_col] > self.filter
                    filtered_count = (~mask).sum()

                    if filtered_count > 0:
                        self.logger.info(
                            f"   Отфильтровано записей по {salary_col} "
                            f"(зарплата <= {self.filter}): {filtered_count}")
                        salary_stats['filtered_count'] += int(filtered_count)
                        processed_df = processed_df[mask]
                    else:
                        self.logger.info(f"   Все записи по {salary_col} имеют зарплату > "
                                         f"{self.filter}")

                    # Статистика после фильтрации
                    if len(processed_df) > 0:
                        salary_after = processed_df[salary_col].describe()
                        self.logger.info(f"   Статистика по {salary_col} после фильтрации:")
                        self.logger.info(f"     Мин: {salary_after.get('min', 'N/A'):.2f}")
                        self.logger.info(f"     Макс: {salary_after.get('max', 'N/A'):.2f}")
                        self.logger.info(f"     Среднее: {salary_after.get('mean', 'N/A'):.2f}")

            # Шаг 4: Дополнительная очистка
            # Удаление строк с пустыми значениями в важных колонках
            if salary_columns:
                processed_df = processed_df.dropna(subset=salary_columns)
                self.logger.info(f"   Удалено записей с зарплатой <="
                                 f" {self.filter}: {initial_count - len(processed_df)}")

            # Шаг 5: Логирование результата
            self.logger.info(f"   Итоговая статистика:")
            self.logger.info(f"     Было записей: {initial_count}")
            self.logger.info(f"     Стало записей: {len(processed_df)}")
            self.logger.info(f"     Отфильтровано по зарплате: {salary_stats['filtered_count']}")

            return processed_df, salary_stats

        except Exception as e:
            self.logger.error(f"Ошибка обработки данных: {e}")
            return df, salary_stats

    def _find_salary_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Поиск колонок с зарплатой в DataFrame.
        """
        salary_keywords = ['salary', 'зарплата', 'оклад', 'income', 'доход', 'pay', 'wage', 'compensation']
        salary_columns = []

        for col in df.columns:
            col_lower = str(col).lower()

            # Проверка по ключевым словам
            for keyword in salary_keywords:
                if keyword in col_lower:
                    salary_columns.append(col)
                    break

            # Дополнительная проверка: если колонка числовая и имя похоже на зарплату
            if col not in salary_columns and pd.api.types.is_numeric_dtype(df[col]):
                # Проверяем диапазон значений (зарплата обычно в разумных пределах)
                try:
                    col_min = df[col].min()
                    col_max = df[col].max()

                    # Зарплата обычно в пределах от 0 до max_threshold
                    if (0 <= col_min <= self.max_threshold
                            and 0 <= col_max <= self.max_threshold):
                        # И имя колонки не похоже на ID или возраст
                        if not any(x in col_lower for x in ['id', 'age', 'возраст', 'код', 'номер']):
                            salary_columns.append(col)
                except:
                    pass

        return list(set(salary_columns))  # Убираем дубликаты

    async def _save_temp_file(self, df: pd.DataFrame, original_file: Path, result: Dict) -> Optional[Path]:
        """
        Сохранение обработанных данных во временный файл.
        """
        try:
            # Создаем уникальное имя файла
            timestamp = int(time.time())
            original_name = original_file.stem

            # Добавляем статистику в имя файла
            filtered_count = result.get('filtered_by_salary', 0)
            total_count = result.get('records_processed', 0)

            temp_filename = f"salary_filtered_{original_name}_total{total_count}_filtered{filtered_count}_{timestamp}.csv"
            temp_file = self.temp_folder / temp_filename

            # Сохраняем в CSV с дополнительной информацией
            with open(temp_file, 'w', encoding='utf-8') as f:
                # Записываем заголовок с информацией о фильтрации
                f.write(f"# Файл отфильтрован по зарплате (> {self.filter})\n")
                f.write(f"# Исходный файл: {original_file.name}\n")
                f.write(f"# Время обработки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Всего записей: {total_count}\n")
                f.write(f"# Отфильтровано по зарплате: {filtered_count}\n")
                f.write(f"# Осталось записей: {len(df)}\n")
                f.write(f"# Порог фильтрации: > {self.filter}\n")
                f.write(f"#\n")

            # Сохраняем данные
            df.to_csv(temp_file, mode='a', index=False, encoding='utf-8')

            self.logger.info(f"   📝 Временный файл сохранен: {temp_file.name}")
            self.logger.info(f"   📊 Размер файла: {temp_file.stat().st_size} байт")

            return temp_file

        except Exception as e:
            self.logger.error(f"Ошибка сохранения временного файла: {e}")
            return None

    async def _move_original_file(self, file_path: Path) -> None:  # ← ВСТАВЬТЕ ЗДЕСЬ
        """
        Перемещение или архивирование исходного файла.
        """
        try:
            # Создаем папку для архива по дате
            archive_date = datetime.now().strftime('%Y-%m-%d')
            archive_folder = self.processed_folder / "archive" / archive_date
            archive_folder.mkdir(parents=True, exist_ok=True)

            # Копируем файл в архив (вместо перемещения)
            archive_file = archive_folder / file_path.name

            # Если файл уже существует, добавляем timestamp
            if archive_file.exists():
                timestamp = int(time.time())
                new_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
                archive_file = archive_folder / new_name

            # Используем shutil для копирования
            import shutil
            shutil.copy2(file_path, archive_file)

            # Удаляем исходный файл только после успешного копирования
            try:
                file_path.unlink()
                self.logger.info(
                    f"   📦 Исходный файл скопирован в архив и удален: {archive_file.relative_to(self.processed_folder)}")
            except:
                # Если не удалось удалить, хотя бы заархивировали
                self.logger.warning(
                    f"   📦 Исходный файл скопирован в архив, но не удален: {archive_file.relative_to(self.processed_folder)}")

        except Exception as e:
            self.logger.error(f"Ошибка архивации файла {file_path}: {e}")

    async def log_pipeline_result(self, result: Dict[str, Any]) -> None:
        """
        Логирование результатов обработки.
        """
        try:
            log_file = self.log_folder / f"pipeline_log_{datetime.now().strftime('%Y-%m-%d')}.json"

            # Читаем существующие логи
            logs = []
            if log_file.exists():
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        logs = json.load(f)
                except:
                    logs = []

            # Добавляем новый лог
            logs.append(result)

            # Сохраняем обновленные логи
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, ensure_ascii=False, indent=2)

            # Загружаем логи в S3 с версионированием
            s3_log_path = (f"logs/"
                           f"pipeline_log_{datetime.now().strftime('%Y-%m-%d')}.json")
            await self.s3_client.upload(str(log_file), s3_log_path)
            await self.s3_client.upload_with_versioning(str(log_file), s3_log_path)

            self.logger.info(f"   📋 Логи сохранены: {log_file.name} -> {s3_log_path}")

        except Exception as e:
            self.logger.error(f"Ошибка логирования: {e}")

    async def process_existing_files(self) -> None:
        """
        Обработка существующих файлов в папке incoming.
        """
        files = list(self.watch_folder.glob("*.*"))
        if not files:
            self.logger.info("📭 В папке incoming нет файлов для обработки")
            return

        self.logger.info(f"🔍 Найдено файлов для обработки: {len(files)}")

        for file_path in files:
            if file_path.is_file():
                # Пропускаем временные файлы и логи
                if file_path.name.startswith(('.', '~', 'temp_')) or file_path.suffix in ['.log', '.tmp']:
                    continue

                result = await self.process_file(file_path)
                await self.log_pipeline_result(result)

                # Пауза между обработкой файлов
                await asyncio.sleep(1)
