import hashlib
import logging
import os
from datetime import timedelta
from io import BytesIO

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


LOGGER = logging.getLogger(__name__)

COLUMN_TYPES = {
    "flat_id": "Int64",
    "building_id": "Int64",
    "floor": "Int64",
    "is_apartment": "boolean",
    "kitchen_area": "float64",
    "living_area": "float64",
    "rooms": "Int64",
    "studio": "boolean",
    "total_area": "float64",
    "price": "Int64",
    "build_year": "Int64",
    "building_type_int": "Int64",
    "latitude": "float64",
    "longitude": "float64",
    "ceiling_height": "float64",
    "flats_count": "Int64",
    "floors_total": "Int64",
    "has_elevator": "boolean",
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.flats_dataset (
    flat_id BIGINT PRIMARY KEY,
    building_id BIGINT,
    floor BIGINT,
    is_apartment BOOLEAN,
    kitchen_area DOUBLE PRECISION,
    living_area DOUBLE PRECISION,
    rooms BIGINT,
    studio BOOLEAN,
    total_area DOUBLE PRECISION,
    price BIGINT,
    build_year BIGINT,
    building_type_int BIGINT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    ceiling_height DOUBLE PRECISION,
    flats_count BIGINT,
    floors_total BIGINT,
    has_elevator BOOLEAN
)
"""


def require_env(names):
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise ValueError(
            "Не заполнены переменные: " + ", ".join(missing)
        )


def make_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    require_env([
        "DB_DESTINATION_HOST",
        "DB_DESTINATION_PORT",
        "DB_DESTINATION_NAME",
        "DB_DESTINATION_USER",
        "DB_DESTINATION_PASSWORD",
    ])

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["DB_DESTINATION_USER"],
        password=os.environ["DB_DESTINATION_PASSWORD"],
        host=os.environ["DB_DESTINATION_HOST"],
        port=int(os.environ["DB_DESTINATION_PORT"]),
        database=os.environ["DB_DESTINATION_NAME"],
    )

    return create_engine(
        url,
        connect_args={"connect_timeout": 10},
        pool_pre_ping=True,
    )


def make_s3_client():
    import boto3
    from botocore.config import Config

    require_env([
        "S3_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
        "S3_BUCKET_NAME",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ])

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=Config(
            signature_version="s3v4",
            connect_timeout=10,
            read_timeout=60,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


def write_csv_to_s3(client, frame, key):
    # Nullable-типы сохраняют пропуски в целых и логических колонках.
    types = {
        column: COLUMN_TYPES[column]
        for column in frame.columns
    }
    frame = frame.astype(types)
    payload = frame.to_csv(index=False).encode("utf-8")

    client.put_object(
        Bucket=os.environ["S3_BUCKET_NAME"],
        Key=key,
        Body=payload,
        ContentType="text/csv; charset=utf-8",
    )

    LOGGER.info("Сохранён объект %s; строк: %s.", key, len(frame))


def read_csv_from_s3(client, key):
    import pandas as pd

    response = client.get_object(
        Bucket=os.environ["S3_BUCKET_NAME"],
        Key=key,
    )

    body = response["Body"]
    try:
        payload = body.read()
    finally:
        body.close()

    # Сначала читаем заголовок, чтобы выбрать типы имеющихся колонок.
    columns = pd.read_csv(BytesIO(payload), nrows=0).columns
    types = {column: COLUMN_TYPES[column] for column in columns}

    return pd.read_csv(
        BytesIO(payload),
        dtype=types,
        float_precision="round_trip",
    )


@dag(
    dag_id="collect_flats_dataset",
    description="Сбор датасета квартир с промежуточным хранением в S3",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mle-user",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["real_estate", "stage_1", "s3"],
)
def collect_flats_dataset():

    @task
    def create_table():
        from sqlalchemy import text

        engine = make_engine()
        try:
            with engine.begin() as conn:
                conn.execute(text(CREATE_TABLE_SQL))
        finally:
            engine.dispose()

        LOGGER.info("Таблица public.flats_dataset готова.")

    @task(multiple_outputs=False)
    def extract():
        import pandas as pd
        from sqlalchemy import text

        run_id = get_current_context()["run_id"]
        run_key = hashlib.sha256(run_id.encode()).hexdigest()
        prefix = f"mle-project/collect_flats_dataset/{run_key}"

        engine = make_engine()
        try:
            # Читаем обе таблицы из одного согласованного снимка БД.
            with engine.connect().execution_options(
                isolation_level="REPEATABLE READ"
            ) as conn:
                with conn.begin():
                    flats = pd.read_sql_query(text("""
                        SELECT
                            id AS flat_id,
                            building_id,
                            floor,
                            is_apartment,
                            kitchen_area,
                            living_area,
                            rooms,
                            studio,
                            total_area,
                            price
                        FROM public.flats
                        ORDER BY id
                    """), conn)

                    buildings = pd.read_sql_query(text("""
                        SELECT
                            id AS building_id,
                            build_year,
                            building_type_int,
                            latitude,
                            longitude,
                            ceiling_height,
                            flats_count,
                            floors_total,
                            has_elevator
                        FROM public.buildings
                        ORDER BY id
                    """), conn)
        finally:
            engine.dispose()

        if flats.empty or buildings.empty:
            raise ValueError("Одна из исходных таблиц пуста.")

        for frame, key in [
            (flats, "flat_id"),
            (buildings, "building_id"),
        ]:
            if frame[key].isna().any() or frame[key].duplicated().any():
                raise ValueError(f"Пустые или повторяющиеся значения {key}.")

        # Не допускаем сопоставления пустых ключей при pandas.merge.
        if flats["building_id"].isna().any():
            raise ValueError("У части квартир отсутствует building_id.")

        paths = {
            "flats": f"{prefix}/flats.csv",
            "buildings": f"{prefix}/buildings.csv",
            "dataset": f"{prefix}/dataset.csv",
        }

        client = make_s3_client()
        try:
            write_csv_to_s3(client, flats, paths["flats"])
            write_csv_to_s3(client, buildings, paths["buildings"])
        finally:
            client.close()

        LOGGER.info(
            "Извлечено квартир: %s; домов: %s.",
            len(flats),
            len(buildings),
        )

        return paths

    @task
    def transform(paths):
        client = make_s3_client()
        try:
            flats = read_csv_from_s3(client, paths["flats"])
            buildings = read_csv_from_s3(client, paths["buildings"])

            dataset = flats.merge(
                buildings,
                on="building_id",
                how="left",
                validate="many_to_one",
                indicator=True,
            )

            unmatched = int((dataset["_merge"] == "left_only").sum())
            LOGGER.info(
                "Квартир без соответствующего дома: %s.",
                unmatched,
            )

            dataset = dataset.drop(columns="_merge")

            if len(dataset) != len(flats):
                raise ValueError("После объединения изменилось число квартир.")

            # Фиксируем порядок всех 18 колонок.
            dataset = dataset[list(COLUMN_TYPES)]

            write_csv_to_s3(client, dataset, paths["dataset"])
        finally:
            client.close()

        LOGGER.info(
            "Датасет собран: %s строк, %s колонок.",
            *dataset.shape,
        )

        return paths["dataset"]

    @task
    def load(dataset_key):
        from sqlalchemy import text

        client = make_s3_client()
        try:
            dataset = read_csv_from_s3(client, dataset_key)
        finally:
            client.close()

        if dataset.empty:
            raise ValueError("Загрузка пустого датасета запрещена.")

        engine = make_engine()
        try:
            # При ошибке удаления/вставки вся транзакция откатится.
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM public.flats_dataset"))

                dataset.to_sql(
                    name="flats_dataset",
                    schema="public",
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )

                actual_count = conn.execute(text("""
                    SELECT COUNT(*) FROM public.flats_dataset
                """)).scalar_one()

                if actual_count != len(dataset):
                    raise ValueError(
                        f"Ожидалось {len(dataset)} строк, "
                        f"загружено {actual_count}."
                    )
        finally:
            engine.dispose()

        LOGGER.info(
            "В public.flats_dataset сохранено %s строк.",
            actual_count,
        )

        return actual_count

    table_ready = create_table()
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)

    table_ready >> extracted


collect_flats_dataset()
