#автоматизируем эту очистку через Airflow

import hashlib
import json
import logging
import os
from datetime import timedelta
from io import BytesIO

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


LOGGER = logging.getLogger(__name__)

# Те же параметры, с которыми очистка проверена в ноутбуке.
CLEANING_PARAMS = {
    "min_price_per_m2": 1000.0,
    "max_price_per_m2": 10_000_000.0,
    "area_tolerance": 0.1,
    "max_ceiling_height": 10.0,
}

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


def make_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    url = URL.create(
        "postgresql+psycopg2",
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


def write_frame(client, frame, key):
    client.put_object(
        Bucket=os.environ["S3_BUCKET_NAME"],
        Key=key,
        Body=frame.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv; charset=utf-8",
    )

    LOGGER.info("Сохранён объект %s; строк: %s.", key, len(frame))


def read_frame(client, key):
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

    return pd.read_csv(
        BytesIO(payload),
        dtype=COLUMN_TYPES,
        float_precision="round_trip",
    )


@dag(
    dag_id="clean_flats_dataset",
    description="Очистка датасета квартир с отчётом и архивом в S3",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mle-user",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["real_estate", "stage_2", "s3"],
)
def clean_flats_dataset():

    @task
    def create_table():
        from sqlalchemy import text

        engine = make_engine()
        try:
            with engine.begin() as conn:
                # Копируем структуру, включая первичный ключ.
                # Строки исходной таблицы эта команда не копирует.
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS public.flats_dataset_clean (
                        LIKE public.flats_dataset INCLUDING ALL
                    )
                """))
        finally:
            engine.dispose()

    @task
    def extract():
        import pandas as pd
        from sqlalchemy import text

        engine = make_engine()
        try:
            with engine.connect() as conn:
                frame = pd.read_sql_query(text("""
                    SELECT *
                    FROM public.flats_dataset
                    ORDER BY flat_id
                """), conn)
        finally:
            engine.dispose()

        if frame.empty:
            raise ValueError("Исходный датасет пуст.")

        run_id = get_current_context()["run_id"]
        run_key = hashlib.sha256(run_id.encode()).hexdigest()
        prefix = f"mle-project/clean_flats_dataset/{run_key}"

        client = make_s3_client()
        try:
            write_frame(client, frame, f"{prefix}/raw.csv")
        finally:
            client.close()

        return prefix

    @task(multiple_outputs=False)
    def transform(prefix):
        # Используем тот же модуль, который создан из ноутбука.
        from real_estate_cleaning import clean_dataset

        client = make_s3_client()
        try:
            frame = read_frame(client, f"{prefix}/raw.csv")

            cleaned, rejected, report = clean_dataset(
                frame,
                **CLEANING_PARAMS,
            )

            if cleaned.empty:
                raise ValueError("После очистки не осталось строк.")

            missing = {
                column: int(count)
                for column, count in cleaned.isna().sum().items()
            }

            audit = {
                "dag_id": "clean_flats_dataset",
                "run_id": get_current_context()["run_id"],
                "source_key": f"{prefix}/raw.csv",
                "cleaning_params": CLEANING_PARAMS,
                "counts": report,
                "missing_after": missing,
            }

            clean_key = f"{prefix}/clean.csv"

            write_frame(client, cleaned, clean_key)
            write_frame(client, rejected, f"{prefix}/rejected.csv")

            client.put_object(
                Bucket=os.environ["S3_BUCKET_NAME"],
                Key=f"{prefix}/report.json",
                Body=json.dumps(
                    audit,
                    ensure_ascii=False,
                    indent=2,
                    allow_nan=False,
                ).encode("utf-8"),
                ContentType="application/json; charset=utf-8",
            )
        finally:
            client.close()

        LOGGER.info("Отчёт очистки: %s", report)

        return {
            "clean_key": clean_key,
            "expected_rows": int(len(cleaned)),
            "missing": missing,
        }

    @task
    def load(metadata):
        from sqlalchemy import text

        client = make_s3_client()
        try:
            frame = read_frame(client, metadata["clean_key"])
        finally:
            client.close()

        if frame.empty or len(frame) != metadata["expected_rows"]:
            raise ValueError(
                "Число строк в S3 не соответствует результату очистки."
            )

        missing = {
            column: int(count)
            for column, count in frame.isna().sum().items()
        }

        if missing != metadata["missing"]:
            raise ValueError(
                "При передаче через CSV изменились пропуски."
            )

        engine = make_engine()
        try:
            # Обновление и проверка результата — одна транзакция.
            # При ошибке предыдущее содержимое таблицы сохранится.
            with engine.begin() as conn:
                conn.execute(text(
                    "DELETE FROM public.flats_dataset_clean"
                ))

                frame.to_sql(
                    "flats_dataset_clean",
                    con=conn,
                    schema="public",
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )

                stats = dict(conn.execute(text("""
                    SELECT
                        COUNT(*) AS rows_count,
                        COUNT(DISTINCT flat_id) AS unique_flats,
                        COUNT(*) - COUNT(living_area) AS living_area_nulls,
                        COUNT(*) - COUNT(kitchen_area) AS kitchen_area_nulls,
                        COUNT(*) - COUNT(ceiling_height) AS ceiling_height_nulls
                    FROM public.flats_dataset_clean
                """)).mappings().one())

                if (
                    stats["rows_count"] != len(frame)
                    or stats["unique_flats"] != len(frame)
                ):
                    raise ValueError(
                        "Не совпало число строк или уникальных квартир."
                    )

                for column in [
                    "living_area",
                    "kitchen_area",
                    "ceiling_height",
                ]:
                    if stats[f"{column}_nulls"] != missing[column]:
                        raise ValueError(
                            f"Не совпало число NULL в {column}."
                        )
        finally:
            engine.dispose()

        LOGGER.info(
            "Проверка public.flats_dataset_clean: %s",
            stats,
        )

        return stats

    ready = create_table()
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)

    ready >> extracted


clean_flats_dataset()
