from sklearn.model_selection import GroupShuffleSplit

from src.common import RAW, TRAIN, TEST, SPLIT_REPORT
from src.common import get_xy, load_params, read_frame, write_frame, write_json


def main():
    params = load_params()
    frame = read_frame(RAW)

    # Проверяем признаки и целевую переменную без обучения преобразований.
    get_xy(frame, params)

    if frame["building_id"].isna().any() or frame["building_id"].nunique() < 2:
        raise ValueError("Для разделения нужны хотя бы два дома с заполненными ID.")

    splitter = GroupShuffleSplit(n_splits=1, **params["split"])
    train_index, test_index = next(
        splitter.split(frame, groups=frame["building_id"])
    )

    train = frame.iloc[train_index].copy()
    test = frame.iloc[test_index].copy()

    overlap = set(train["building_id"]) & set(test["building_id"])
    if (
        overlap
        or len(train) + len(test) != len(frame)
        or min(len(train), len(test)) < 2
    ):
        raise ValueError("Некорректное разделение данных.")

    write_frame(train, TRAIN)
    write_frame(test, TEST)

    report = {
        "total_rows": int(len(frame)),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_buildings": int(train["building_id"].nunique()),
        "test_buildings": int(test["building_id"].nunique()),
        "overlapping_buildings": len(overlap),
        "test_fraction_rows": float(len(test) / len(frame)),
    }
    write_json(report, SPLIT_REPORT)
    print(report)


if __name__ == "__main__":
    main()
