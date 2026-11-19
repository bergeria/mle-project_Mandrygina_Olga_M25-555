import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.engine import URL

from src.common import RAW, ROOT, load_params, write_frame


def main():
    load_dotenv(ROOT / ".env")

    names = [
        f"DB_DESTINATION_{part}"
        for part in ("HOST", "PORT", "NAME", "USER", "PASSWORD")
    ]
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise ValueError("Не заполнены переменные: " + ", ".join(missing))

    params = load_params()["data"]
    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ["DB_DESTINATION_USER"],
        password=os.environ["DB_DESTINATION_PASSWORD"],
        host=os.environ["DB_DESTINATION_HOST"],
        port=int(os.environ["DB_DESTINATION_PORT"]),
        database=os.environ["DB_DESTINATION_NAME"],
    )
    engine = create_engine(url, connect_args={"connect_timeout": 10})

    try:
        with engine.connect() as connection:
            table = Table(
                params["table"],
                MetaData(),
                schema=params["schema"],
                autoload_with=connection,
            )
            query = select(table).order_by(table.c.flat_id)
            frame = pd.read_sql_query(query, connection)
    finally:
        engine.dispose()

    if frame.empty:
        raise ValueError("Исходная таблица пуста.")

    if frame[["flat_id", "building_id"]].isna().any().any():
        raise ValueError("Есть пропуски в идентификаторах.")

    if frame["flat_id"].duplicated().any():
        raise ValueError("Есть повторяющиеся flat_id.")

    write_frame(frame, RAW)
    print(f"Выгружено строк: {len(frame):,}")


if __name__ == "__main__":
    main()
