import json

import joblib
import numpy as np
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
    r2_score,
)
from threadpoolctl import threadpool_limits

from src.common import TEST, MODEL, BASELINE, METRICS
from src.common import get_xy, load_params, read_frame, write_json


def main():
    params = load_params()
    X, y = get_xy(read_frame(TEST), params)
    metrics = {}

    for name, path in {"model": MODEL, "baseline": BASELINE}.items():
        estimator = joblib.load(path)

        with threadpool_limits(limits=params["model"]["n_threads"]):
            prediction = estimator.predict(X)

        if not np.isfinite(prediction).all():
            raise ValueError(
                f"Модель {name} вернула некорректные прогнозы."
            )

        metrics[name] = {
            "mae_rub": float(mean_absolute_error(y, prediction)),
            "rmse_rub": float(root_mean_squared_error(y, prediction)),
            "mape_pct": float(
                100 * mean_absolute_percentage_error(y, prediction)
            ),
            "r2": float(r2_score(y, prediction)),
        }

    write_json(metrics, METRICS)
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
