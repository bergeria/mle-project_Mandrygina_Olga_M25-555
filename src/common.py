import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/flats_clean.csv"
TRAIN = ROOT / "data/processed/train.csv"
TEST = ROOT / "data/processed/test.csv"
MODEL = ROOT / "models/price_model.joblib"
BASELINE = ROOT / "models/dummy_model.joblib"
METRICS = ROOT / "reports/metrics.json"
SPLIT_REPORT = ROOT / "reports/split.json"


def load_params():
    with (ROOT / "params.yaml").open(encoding="utf-8") as file:
        return yaml.safe_load(file)


def read_frame(path):
    return pd.read_csv(path, float_precision="round_trip")


def write_frame(frame, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def write_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def get_xy(frame, params):
    columns = params["features"]["numeric"] + params["features"]["categorical"]
    forbidden = {"price", "price_per_m2", "flat_id", "building_id", "studio"}

    if forbidden.intersection(columns) or len(columns) != len(set(columns)):
        raise ValueError("Недопустимый или повторяющийся признак.")

    # Булевы признаки преобразуются в 0/1, пропуски пока сохраняются.
    X = frame[columns].astype("float64")
    y = frame["price"].astype("float64")

    if len(frame) == 0 or not np.isfinite(y).all() or y.le(0).any():
        raise ValueError("Пустой датасет или некорректная целевая переменная.")

    if np.isinf(X.to_numpy()).any():
        raise ValueError("В признаках обнаружены бесконечные значения.")

    return X, y
