# Работа с объектным хранилищем
## Выполнить задания: 
### Первое задание:

1) Добавить метод `list_files()`, который будет возвращать список объектов в бакете.
2) Добавить метод `file_exists()`, который должен возвращать булевый ответ на запрос о наличии файла с определенным именем.

Методы должны корректно принимать параметры, возвращать ответ, код должен быть рабочим (скриншот/запись экрана/передача credentials для подключения).

### Второе задание:

1) Настроить `bucket policy` таким образом, чтобы:
2) Любой может читать файлы из определенного бакета.
3) Только вы можете писать в него.
4) Включить версионирование в бакете и загрузить файл с одним именем несколько раз, а после скачайть его предыдущую версию.
5) Настроить `lifecycle policy` таким образом, чтобы через 3 дня объекты автоматически удалялись.
6) Критерии приема: скриншот/запись экрана/передача credentials для подключения - каждый из пунктов подтверждает успешную работу.

Данное задание можно выполнить двумя способами: с помощью `boto3`, путем отправки запроса на `API`, а также через интерфейс `Selectel`.

### Третье задание:

#### Создать автоматизированный пайплайн, который:

1) Следит за указанной локальной папкой: использует `watchdog`, `watchfiles` для отслеживания новых файлов. Появился новый? Запускается пайплайн. 
2) Обслуживает новые файлы с данными: читает с `pandas.DataFrame`, выполняет фильтрацию по любому из придуманных условий, сохраняет во временный файл.
3) Заливает его в хранилище: асинхронно загружает полученный файл в указанную папку бакета.
4) Перемещает обработанные файлы: либо удаляет исходный файл, либо помещает в архив.
5) Логирует все этапы работы:
    - Записывает логи в отдельный файл.
    - Перезаписывает его в хранилище (используя версионирование).

## Реализация:
### Реализация в среде `Selectel Cloud Storage`:
1) Настраиваю `bucket policy` таким образом, чтобы любой может читать файлы из определенного бакета и только я могу писать в него (`account_root`), скриншот (Access_policy.png): </br>
<img width="576" height="436" alt="image" src="https://github.com/user-attachments/assets/65980e36-c4a5-43a3-a585-9de9e87f613c" /></br>
2) Создаю бакет в `Selectel Cloud Storage` скриншот (Create_backet_in_Selectel.png) с включением версионирования:</br>
<img width="508" height="536" alt="image" src="https://github.com/user-attachments/assets/73e970fe-c11a-4d1f-b54e-ff78fab3bcf3" /></br>
3) Настраиваю `lifecycle policy` таким образом, чтобы через 3 дня объекты автоматически удалялись, скриншот (Lyfecycle_policy.png):</br>
<img width="548" height="341" alt="image" src="https://github.com/user-attachments/assets/86f7552c-00db-4b0c-955a-42ed2e3e9133" /></br>
4) Скриншот (Empty_bucket.png) пустого бакета:</br>
<img width="974" height="332" alt="image" src="https://github.com/user-attachments/assets/122a7959-f031-4c3e-b526-6c111b25e02c" /></br>
5) Скриншот (File_1767456594604087595_recovery_step1.png) несколько версий одного файла в бакете:</br>
<img width="761" height="436" alt="image" src="https://github.com/user-attachments/assets/983fec5a-b050-48ff-a021-3bfce65b1c51" /></br>
7) Скриншот (File_1767456594604087595_recovery_step2.png) восстановим одну из версий файла в бакете:</br>     
<img width="627" height="437" alt="image" src="https://github.com/user-attachments/assets/c8f763bc-82ca-4db8-8cdb-126907bf0c65" /></br>
8) Скриншот (File_1767456594604087595_recovery_step3.png) восстановили одну из версий файла в бакете:</br>
<img width="645" height="331" alt="image" src="https://github.com/user-attachments/assets/6b4b8db3-a7b8-4eff-963a-f3db6928b2ae" /></br>
9) Затем нужно для восстановленного файла demo_versioning.txt нажать справа 3 вертикальные точки и выбрать Скачать.</br>

### Реализация проекта:    
1) Структура проекта:
```
├── config/
│   └── config.py
├── data/
│   └── incoming/      # Папка для файлов, которые нужно обработать
│   └── logs/
│   │   └── pipeline_log_2026-01-08.json  
|   │   └── pipeline_task3_20260108_195619.log
│   │   └── pipeline_tasks_1&2_20260108_194148.log
│   └── processed/
|   |   └── archive/
|   |   |   └── 2026-01-08/
|   |   │   │   └── employees_example.csv # Исходный файл, который обрабатывался в папке incoming
|   │   |   |   └── salary_filtered_employees_example_1767891380.csv # Полученный файл, в результате работы пайплайна
|   │   |   |   └── salary_filtered_employees_example_1767891434.csv # Полученный файл, в результате работы пайплайна
|   │   |   |   └── salary_filtered_test_data_1767891387.csv # Полученный файл, в результате работы пайплайна
|   │   |   |   └── test_data.csv # Исходный файл, который обрабатывался в папке incoming
│   └── temp/
│   │   └── demo_versioning_previous_v_1767635318884375389.txt  # Скачанная предыдущая версия файла 
|   │   └── demo_versioning_v1.txt
│   │   └── demo_versioning_v2.txt
│   │   └── demo_versioning_v3.txt
├── screenshots/
│   └── Access_policy.png
|   └── Create_backet_into_Selectel.png
│   └── Empty_bucket.png
│   └── File_1767456594604087595_recovery_step1.png
│   └── File_1767456594604087595_recovery_step2.png
│   └── File_1767456594604087595_recovery_step3.png
│   └── Lyfecycle_policy.png
│   └── Selectel_filtered_files_task3.png
│   └── Selectel_log_task3.png
│   └── Tasks_1_2.png
├── src/
│   └── async_s3_client.py
|   └── pipeline.py
├── tests/
│   └── test_config.py
|   └── test_selectel_connection.py
├── .env
├── requirements.txt
├── run_pipeline_task1&2.py
├── run_pipeline_task3.py
```   
3) Методы `list_files()` и `file_exists()` добавлены в скрипт `async_s3_client.py`, который находится в папке `src`.
4) Для выполнения заданий 1 и 2 можно запустить скрипт `run_pipeline_task1&2.py`. Файлы для скрипта сформируются автоматически. </br>
Результатом работы скрипта будут логи вида: </br>
`pipeline_tasks_1&2_20260108_194148.log` в директории: **data/logs**. </br>
```
├── data/
│   └── logs/
│   │   └── pipeline_log_2026-01-08.json  
|   │   └── pipeline_task3_20260108_195619.log
│   │   └── pipeline_tasks_1&2_20260108_194148.log
```
Результат работы скрипта для 1 и 2 заданий, как пример, находится в папке *logs*. Кроме этого, файлы, скаченные из S3, 
можно посмотреть в директории ** temp.**</br>
```
├── data/
│   └── temp/
│   │   └── demo_versioning_previous_v_1767635318884375389.txt  # Скачанная предыдущая версия файла 
|   │   └── demo_versioning_v1.txt
│   │   └── demo_versioning_v2.txt
│   │   └── demo_versioning_v3.txt
```
В Selectel все выглядит так, скриншот (Tasks_1_2.png):</br>
<img width="768" height="398" alt="image" src="https://github.com/user-attachments/assets/45d740b6-eae0-44c2-9550-d3f6fe4e1570" /></br>

3) Для выполнения задания 3 необходимо запустить скрипт `run_pipeline_task3.py` он работает со скриптом `pipeline.py`, который находится в папке `src`.</br>
Результатом работы скрипта будут логи вида: </br>
`pipeline_task3_20260108_195619.log` в директории: **data/logs**. </br>
```
├── data/
│   └── logs/
│   │   └── pipeline_log_2026-01-08.json  
|   │   └── pipeline_task3_20260108_195619.log
```
Логи в `Selectel` выглядят так, скриншот (Selectel_log_task3.png): </br>
<img width="775" height="403" alt="image" src="https://github.com/user-attachments/assets/71d51e44-d847-49ed-a56f-98f68573aa59" /></br>
a) Сначала запускаем `run_pipeline_task3.py`. </br>
b) Затем, в директорию **data/incoming** копируем файл со столбцом `salary` для того, чтобы получилось отфильтровать данные. </br>
c) Параметры фильтра по зарплате необходимо указать в файле `config.py` директории `config`. </br>
Сейчас установлено значение: `filter_threshold`: 55000. </br> 
Т.е. мы сформируем файлы, в которых зарплата будет больше или равна указанного значения. </br>
В Selectel они выглядят так, скриншот (Selectel_filtered_files_task3.png):</br>
<img width="769" height="474" alt="image" src="https://github.com/user-attachments/assets/17bbdf05-1feb-4fb2-bbb1-b99a6c2e7cfd" /></br>
d) В локальной директории в архиве лежат исходные файлы, которые можно скопировать в **incoming**, и файлы, полученные после работы пайплана. </br>
Они получились в результате работы пайплана по фильтрации исходных файлов по зарплате. </br>
```
├── data/
│   └── processed/
|   |   └── archive/
|   |   |   └── 2026-01-08/
|   |   │   │   └── employees_example.csv # Исходный файл, который обрабатывался в папке incoming
|   │   |   |   └── salary_filtered_employees_example_1767891380.csv # Полученный файл, в результате работы пайплайна
|   │   |   |   └── salary_filtered_employees_example_1767891434.csv # Полученный файл, в результате работы пайплайна
|   │   |   |   └── salary_filtered_test_data_1767891387.csv # Полученный файл, в результате работы пайплайна
|   │   |   |   └── test_data.csv # Исходный файл, который обрабатывался в папке incoming
```

### Запуск проекта:
```
# 1. Склонировать репозиторий
git clone

# 2. Установка зависимостей
pip install -r requirements.txt

# 3. Настройка окружения
.env
# отредактируйте .env своими значениями

# 4. Запуск 
python run_pipeline_task1&2.py
python run_pipeline_task3.py
```
