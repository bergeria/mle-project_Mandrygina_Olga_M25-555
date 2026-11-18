import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


def main():
    required = [
        "DB_DESTINATION_HOST",
        "DB_DESTINATION_PORT",
        "DB_DESTINATION_NAME",
        "DB_DESTINATION_USER",
        "DB_DESTINATION_PASSWORD",
    ]

    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise ValueError(
            "Не заполнены переменные: " + ", ".join(missing)
        )

    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["DB_DESTINATION_USER"],
        password=os.environ["DB_DESTINATION_PASSWORD"],
        host=os.environ["DB_DESTINATION_HOST"],
        port=int(os.environ["DB_DESTINATION_PORT"]),
        database=os.environ["DB_DESTINATION_NAME"],
    )

    engine = create_engine(
        url,
        connect_args={"connect_timeout": 10},
    )

    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) AS rows_count,
                    COUNT(DISTINCT flat_id) AS unique_flats,
                    COUNT(DISTINCT building_id) AS unique_buildings,
                    COUNT(*) - COUNT(living_area) AS living_area_nulls,
                    COUNT(*) - COUNT(kitchen_area) AS kitchen_area_nulls,
                    COUNT(*) - COUNT(ceiling_height) AS ceiling_height_nulls
                FROM public.flats_dataset_clean
            """)).mappings().one()

        print("Проверка public.flats_dataset_clean")
        print(f"Всего строк:          {result['rows_count']:,}")
        print(f"Уникальных квартир:  {result['unique_flats']:,}")
        print(f"Уникальных домов:    {result['unique_buildings']:,}")
        print()
        print(f"Пропусков living_area:    {result['living_area_nulls']:,}")
        print(f"Пропусков kitchen_area:   {result['kitchen_area_nulls']:,}")
        print(f"Пропусков ceiling_height: {result['ceiling_height_nulls']:,}")

        if result["rows_count"] == 0:
            print("\nТаблица пуста.")
        elif result["rows_count"] == result["unique_flats"]:
            print("\nПустых или повторяющихся flat_id нет.")
        else:
            print("\nОбнаружены пустые или повторяющиеся flat_id.")

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

