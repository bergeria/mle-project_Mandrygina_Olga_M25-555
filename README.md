# Оценка стоимости квартир: Airflow и DVC

Учебный MVP модели регрессии для предсказания цены квартиры по её характеристикам и характеристикам дома.

Проект выполняется на виртуальной машине. PostgreSQL используется для хранения исходных и подготовленных таблиц, S3 — для промежуточных файлов Airflow и артефактов DVC.

## Сбор данных

DAG `collect_flats_dataset` объединяет таблицы `public.flats` и `public.buildings` по условию:

```sql
flats.building_id = buildings.id
```

Шаги DAG: `create_table`, `extract`, `transform`, `load`.

Результат: `public.flats_dataset`, 141 362 квартиры и 24 620 домов.

Оба DAG работают с персональной БД через переменные `DB_DESTINATION_*`.
Поля `DB_SOURCE_*` сохранены в шаблоне окружения, но в текущей реализации не используются.

## Очистка данных

Исследование и функции очистки представлены в `notebooks/02_data_cleaning.ipynb`.
Модуль `dags/real_estate_cleaning.py` используется DAG `clean_flats_dataset`.

Основные правила:

- удаление полных дубликатов;
- сохранение квартир с разными ID и одинаковыми характеристиками;
- исключение строк с некорректной ценой или общей площадью;
- исключение строк с ценой за м² вне диапазона от 1 000 до 10 000 000 рублей;
- замена некорректных жилой площади и площади кухни пропусками;
- проверка согласованности площадей с допуском 0,1 м²;
- замена некорректной высоты потолков, включая значения выше 10 м, пропусками.

Границы цены за м² используются как эвристика проверки качества данных.
Цена за м² не передаётся модели в качестве признака.

После очистки исключено 147 строк.
В `public.flats_dataset_clean` сохранены 141 215 квартир и 24 589 домов.

Пропуски после очистки:

| Признак | Количество |
|---|---:|
| living_area | 18 679 |
| kitchen_area | 11 788 |
| ceiling_height | 1 |

Заполнение пропусков выполняется при обучении модели, по тренировочной выборке.

## Обучение и оценка

DVC-пайплайн содержит четыре шага:

| Шаг | Модуль | Результат |
|---|---|---|
| extract | src/extract.py | Снимок очищенной таблицы |
| split | src/split_data.py | Обучающая и тестовая выборки |
| train | src/train.py | Модель и медианный baseline |
| evaluate | src/evaluate.py | Метрики тестовой выборки |

Параметры находятся в `params.yaml`, зависимости шагов — в `dvc.yaml`.
Файл `dvc.lock` создаётся DVC при исполнении пайплайна.

Данные разделены по `building_id` с помощью `GroupShuffleSplit`.
В тест выделяется 20% домов, `random_state=42`.

| Выборка | Квартиры | Дома |
|---|---:|---:|
| Обучающая | 113 407 | 19 671 |
| Тестовая | 27 808 | 4 918 |

Пересечений домов между выборками нет.

Из признаков исключены идентификаторы, целевая переменная и константный `studio`.
`building_type_int` обрабатывается как категориальный признак.

В единый sklearn Pipeline включены:
- заполнение числовых пропусков медианами с индикаторами пропусков;
- заполнение категориальных пропусков наиболее частым значением;
- One-Hot Encoding;
- HistGradientBoostingRegressor с функцией потерь absolute_error.

Все обучаемые преобразования настраиваются только на тренировочной выборке.
Для сравнения используется DummyRegressor, предсказывающий медиану тренировочных цен.

## Результаты

Основная метрика — MAE в рублях: средняя абсолютная ошибка оценки стоимости.
Дополнительно рассчитываются RMSE, MAPE и R².

| Метрика | Медианный baseline | Модель |
|---|---:|---:|
| MAE, руб. | 10 785 739 | 4 112 728 |
| RMSE, руб. | 36 216 508 | 14 227 150 |
| MAPE, % | 41,35 | 22,68 |
| R² | -0,0495 | 0,8381 |

Снижение MAE относительно baseline — 61,87%.
Точные значения сохранены в `reports/metrics.json`.
Состав выборок описан в `reports/split.json`.

Результаты относятся к очищенному датасету и домам, отсутствовавшим в обучении.
Средняя абсолютная ошибка остаётся около 4,11 млн рублей.
Соответствие прогнозов ценам реальных сделок и влияние на количество сделок требуют отдельной проверки.

## Подготовка окружения

Используются Python 3.10, Docker Compose и Airflow 2.7.3.

Создайте `.env` на основе `.env_template` и заполните параметры персональной БД и S3.
Для Linux укажите в AIRFLOW_UID результат команды `id -u`.
В персональной БД должны быть доступны таблицы buildings и flats и право создавать таблицы.

Docker Compose загружает параметры подключения из файла .env и передаёт их в переменные окружения 
контейнеров Airflow через настройку env_file.
Код DAG использует эти параметры для подключения к PostgreSQL и S3.


Окружение обучения:

```bash
python3 -m venv .venv_ml
source .venv_ml/bin/activate
python -m pip install -r requirements-ml.lock.txt
python -m pip check
```

`requirements-ml.txt` содержит основные зависимости, а `requirements-ml.lock.txt` — версии установленных пакетов окружения обучения.

## Запуск подготовки данных

Первоначальный запуск Airflow:

```bash
mkdir -p dags logs plugins config data/raw data/processed models reports
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose exec airflow-scheduler airflow dags list-import-errors
```

В интерфейсе Airflow последовательно запустите:
1. collect_flats_dataset — дождитесь успешного завершения;
2. clean_flats_dataset — дождитесь успешного завершения.

Проверка очищенной таблицы из окружения обучения:

```bash
dotenv -f .env run -- python check_clean_dataset.py
```

## Запуск обучения и сохранение в S3

Все команды выполняются из корня проекта с активным `.venv_ml`.

Настройка DVC remote и проверка записи/чтения:

```bash
python setup_dvc_s3.py
```

Обучение и оценка:

```bash
dvc repro
dvc metrics show
```

Отправка артефактов и проверка синхронизации:

```bash
dotenv -f .env run -- dvc push
dotenv -f .env run -- dvc status -c
```

Модель вместе с предобработкой сохраняется локально в `models/price_model.joblib`.
Для DVC используется remote `s3_storage` с префиксом `mle-project/dvc` в бакете из S3_BUCKET_NAME.

DVC хранит объекты в S3 под именами, основанными на хешах.
Исходные имена файлов восстанавливаются при загрузке:

```bash
dotenv -f .env run -- dvc pull
```

DVC не обнаруживает изменения содержимого PostgreSQL автоматически.
После обновления таблицы через Airflow снимок и обучение обновляются командами:

```bash
dvc repro --force extract
dvc repro
dotenv -f .env run -- dvc push
```

В Git сохраняются код, конфигурация, ноутбук, метрики и dvc.lock.
Датасеты и модели версионируются через DVC.
Файл .env, окружения Python и рабочие логи исключены из Git.

<!-- LOCAL_SETUP_START -->
## Локальная установка и запуск

### 1. Подготовка компьютера

Инструкция рассчитана на Ubuntu 22.04 и Python 3.10.
На Windows используется Ubuntu 22.04 внутри WSL2.

Если WSL ещё не установлен, выполните в PowerShell от имени администратора:

```powershell
wsl --install -d Ubuntu-22.04
```

После установки и запрошенной перезагрузки откройте Ubuntu
и создайте пользователя.

Установите [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/).
Включите **Use WSL 2 based engine**, затем интеграцию с Ubuntu-22.04
в **Settings → Resources → WSL Integration**.
При таком варианте отдельная установка Docker Engine внутри WSL не требуется.

Для обычной Ubuntu установите
[Docker Engine и Compose plugin](https://docs.docker.com/engine/install/ubuntu/).

Дальнейшие команды выполняются в терминале Ubuntu, включая вариант с WSL2.
Для контейнеров Airflow Docker должно быть доступно минимум 4 ГБ памяти,
желательно 8 ГБ.

```bash
sudo apt update
sudo apt install -y git python3.10 python3.10-venv python3.10-dev build-essential

python3.10 --version
git --version
docker --version
docker compose version
docker info
```

Для полного запуска нужен сетевой доступ к персональной PostgreSQL и S3.
В PostgreSQL должны существовать исходные таблицы `public.buildings`
и `public.flats`. Пользователю БД нужны права чтения исходных таблиц,
а также создания и заполнения таблиц датасетов.

Контейнер `postgres` в Compose хранит служебные данные Airflow.
Исходные таблицы недвижимости в нём автоматически не создаются.

### 2. Клонирование репозитория

Храните проект в домашнем каталоге Ubuntu, например `~/projects`.
Это относится и к Ubuntu внутри WSL2.

```bash
mkdir -p ~/projects
cd ~/projects
git clone --branch main https://github.com/bergeria/mle-project_Mandrygina_Olga_M25-555.git mle-project_Mandrygina_Olga_M25-555
cd mle-project_Mandrygina_Olga_M25-555
```

Для закрытого репозитория потребуется авторизация в GitHub.
Все последующие команды выполняются из корня проекта.
Python-окружения создаются заново на каждой машине.

### 3. Настройка подключений

Создайте `.env` из шаблона и задайте UID текущего пользователя:

```bash
if [ ! -f .env ]; then
    cp .env_template .env
fi

chmod 600 .env
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
```

Заполните `.env` в редакторе:

| Переменные | Назначение |
|---|---|
| `AIRFLOW_UID` | UID пользователя Ubuntu; задаётся командой выше |
| `DB_DESTINATION_HOST`, `DB_DESTINATION_PORT`, `DB_DESTINATION_NAME`, `DB_DESTINATION_USER`, `DB_DESTINATION_PASSWORD` | Персональная БД с исходными и итоговыми таблицами |
| `S3_ENDPOINT_URL`, `AWS_DEFAULT_REGION`, `S3_BUCKET_NAME` | Endpoint, регион и имя бакета |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Ключи доступа к S3 |

Поля `DB_SOURCE_*` сохранены из шаблона курса, но текущие DAG и обучение
используют `DB_DESTINATION_*`. Заголовок `[default]` в `.env` не нужен.

Файл `.env` не добавляется в Git.
Активация Python-окружения сама по себе не загружает `.env`:
скрипты используют `load_dotenv`, а команды DVC с доступом к S3
ниже запускаются через `dotenv`.

### 4. Окружение для обучения и DVC

```bash
python3.10 -m venv .venv_ml
source .venv_ml/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements-ml.lock.txt
python -m pip check
```

В этом окружении выполняются обучение, оценка модели и команды DVC.
`requirements-ml.txt` содержит основные зависимости,
`requirements-ml.lock.txt` — зафиксированные версии установленных пакетов.

### 5. Окружение для ноутбука

Этот шаг нужен для работы с исследованием данных в Jupyter.
Для исполнения готовых DAG и DVC-пайплайна он не обязателен.

```bash
deactivate

python3.10 -m venv .venv_project_name
source .venv_project_name/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements-notebook.lock.txt
python -m pip check

python -m ipykernel install --user --name mle-project-notebook --display-name "MLE project: Notebook"
```

Откройте проект в VS Code с расширениями Python и Jupyter.
На Windows открывайте каталог через расширение WSL.

Для `notebooks/02_data_cleaning.ipynb` выберите ядро
**MLE project: Notebook** или интерпретатор `.venv_project_name/bin/python`.
Путь к интерпретатору можно проверить в ячейке:

```python
import sys
print(sys.executable)
```

Переключение окружения в терминале не переключает уже запущенное ядро Jupyter.

После работы с ноутбуком вернитесь в окружение обучения:

```bash
deactivate
source .venv_ml/bin/activate
```

| Среда | Использование |
|---|---|
| `.venv_project_name` | Ноутбук и исследование данных |
| `.venv_ml` | DVC, обучение и оценка модели |
| Docker-контейнеры Airflow | Исполнение DAG со своими Python-зависимостями |

Окружения взаимодействуют через файлы проекта, PostgreSQL и S3.
Активация любого из двух `venv` не меняет зависимости контейнеров.

### 6. Настройка DVC и S3

В активном `.venv_ml` выполните:

```bash
python setup_dvc_s3.py
```

Скрипт читает `.env`, настраивает remote `s3_storage`
и проверяет запись и чтение тестового объекта.

Повторно выполнять `dvc init` после клонирования не нужно:
каталог `.dvc` уже находится в репозитории.

Далее выберите один из двух сценариев:
восстановление готовых артефактов или полный запуск.

### 7. Восстановление готовой модели и данных

Если артефакты выбранного коммита ранее отправлены в S3 через `dvc push`,
их можно получить без запуска Airflow и подключения к PostgreSQL.

Для этого сценария достаточно Git, окружения `.venv_ml`,
доступа к нужному бакету и шагов 2–4 и 6.

```bash
dotenv -f .env run -- dvc pull
dvc metrics show

ls -lh models/price_model.joblib models/dummy_model.joblib
```

`models/price_model.joblib` содержит обученный Pipeline с предобработкой.

В S3 DVC хранит объекты под именами-хешами.
Исходные пути восстанавливает `dvc pull` по метаданным репозитория.

На этом восстановление закончено.
Следующий раздел нужен для нового запуска подготовки данных и обучения.

### 8. Полный запуск проекта

#### Запуск Airflow

Создайте каталоги до запуска контейнеров:

```bash
mkdir -p dags logs plugins config data/raw data/processed models reports

docker compose config --quiet
docker compose build
docker compose up airflow-init
```

Дождитесь завершения `airflow-init` с кодом 0, затем выполните:

```bash
docker compose up -d
docker compose ps

docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Откройте [Airflow](http://localhost:8080).
По умолчанию логин и пароль — `airflow` / `airflow`,
если они не переопределены через `_AIRFLOW_WWW_USER_USERNAME`
и `_AIRFLOW_WWW_USER_PASSWORD`.

#### Подготовка данных

1. Включите и вручную запустите DAG `collect_flats_dataset`.
2. Дождитесь статуса `success` у всего запуска.
3. Включите и запустите DAG `clean_flats_dataset`.
4. Дождитесь его завершения со статусом `success`.

DAG пересобирают соответственно `public.flats_dataset`
и `public.flats_dataset_clean` в персональной БД.

Проверьте очищенный датасет из активного `.venv_ml`:

```bash
dotenv -f .env run -- python check_clean_dataset.py
```

Для исходных данных проекта ожидается 141 215 строк и 24 589 домов.

#### Обучение и сохранение в S3

```bash
dvc repro --force extract
dvc repro
dvc metrics show

dotenv -f .env run -- dvc push
dotenv -f .env run -- dvc status -c
```

Первый вызов принудительно перечитывает таблицу из БД:
DVC не отслеживает изменения внутри PostgreSQL автоматически.

Следующий вызов выполняет необходимые стадии пайплайна.
Если зависимости и данные не изменились, DVC может использовать
уже полученные результаты.

`dvc push` отправляет данные и модели в S3.
`dvc.lock` создаётся и обновляется командой `dvc repro`;
его сохраняют в Git вместе с кодом и конфигурациями.

### 9. Повторный запуск и остановка

В новом терминале перейдите в каталог проекта:

```bash
source .venv_ml/bin/activate
docker compose up -d
dvc repro
```

Для остановки контейнеров с сохранением их состояния и данных:

```bash
docker compose stop
```

После изменения `.env` примените настройки к контейнерам заново:

```bash
docker compose up -d --force-recreate
```

Если порт 8080 занят другим проектом, выберите свободный порт
в пробросе `airflow-webserver`, например `8081:8080`,
и открывайте `http://localhost:8081`.
<!-- LOCAL_SETUP_END -->
