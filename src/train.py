import joblib
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from threadpoolctl import threadpool_limits

from src.common import TRAIN, MODEL, BASELINE, get_xy, load_params, read_frame


def main():
    params = load_params()
    X, y = get_xy(read_frame(TRAIN), params)

    numeric = SimpleImputer(
        strategy="median",
        add_indicator=True,
        keep_empty_features=True,
    )

    categorical = Pipeline([
        (
            "imputer",
            SimpleImputer(
                strategy="most_frequent",
                keep_empty_features=True,
            ),
        ),
        (
            "encoder",
            OneHotEncoder(
                handle_unknown="ignore",
                sparse_output=False,
            ),
        ),
    ])

    preprocessing = ColumnTransformer([
        ("numeric", numeric, params["features"]["numeric"]),
        ("categorical", categorical, params["features"]["categorical"]),
    ], remainder="drop", sparse_threshold=0)

    model_params = params["model"].copy()
    n_threads = model_params.pop("n_threads")

    model = Pipeline([
        ("preprocessing", preprocessing),
        (
            "regressor",
            HistGradientBoostingRegressor(
                categorical_features=None,
                **model_params,
            ),
        ),
    ])

    # Все обучаемые преобразования получают только тренировочные данные.
    with threadpool_limits(limits=n_threads):
        model.fit(X, y)

    baseline = DummyRegressor(strategy="median")
    baseline.fit(X, y)

    MODEL.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL, compress=3)
    joblib.dump(baseline, BASELINE, compress=3)

    print(f"Обучение завершено. Строк train: {len(X):,}")
    print(f"Модель сохранена: {MODEL}")


if __name__ == "__main__":
    main()
