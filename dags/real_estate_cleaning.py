
#Таким образом, функции останутся видны в ноутбуке,
#а DAG сможет импортировать этот же код.

import numpy as np
import pandas as pd


DATASET_COLUMNS = [
    "flat_id", "building_id", "floor", "is_apartment", "kitchen_area",
    "living_area", "rooms", "studio", "total_area", "price", "build_year",
    "building_type_int", "latitude", "longitude", "ceiling_height",
    "flats_count", "floors_total", "has_elevator",
]


def remove_exact_duplicates(frame):
    """Удаляем только полностью одинаковые строки, включая flat_id."""
    duplicate_mask = frame.duplicated(keep="first")

    rejected = frame.loc[duplicate_mask].copy()
    rejected["reject_reason"] = "exact_duplicate"

    result = frame.loc[~duplicate_mask].copy()

    if result["flat_id"].isna().any():
        raise ValueError("Обнаружен пустой flat_id.")

    if result["flat_id"].duplicated().any():
        raise ValueError("Один flat_id имеет разные характеристики.")

    return result, rejected


def clean_feature_values(frame, area_tolerance=0.1, max_ceiling_height=10.0):
    """Обозначаем неизвестные и противоречивые значения пропусками."""
    result = frame.copy()
    report = {}

    for column in ["living_area", "kitchen_area"]:
        values = result[column].astype("float64")

        invalid = values.notna() & (
            ~np.isfinite(values) | values.le(0)
        )

        report[f"{column}_invalid_to_nan"] = int(invalid.sum())
        result[column] = values.mask(invalid)

        # Сначала исправляем противоречия отдельных площадей.
        too_large = (
            result[column] > result["total_area"] + area_tolerance
        )

        report[f"{column}_exceeds_total"] = int(too_large.sum())
        result.loc[too_large, column] = np.nan

    # Затем проверяем сумму оставшихся известных площадей.
    excess = (
        result["living_area"]
        + result["kitchen_area"]
        - result["total_area"]
    )

    inconsistent = excess > area_tolerance
    report["area_sum_conflicts"] = int(inconsistent.sum())

    result.loc[
        inconsistent,
        ["living_area", "kitchen_area"],
    ] = np.nan

    ceiling = result["ceiling_height"].astype("float64")

    invalid_ceiling = ceiling.notna() & (
        ~np.isfinite(ceiling)
        | ceiling.le(0)
        | ceiling.gt(max_ceiling_height)
    )

    report["ceiling_height_invalid_to_nan"] = int(
        invalid_ceiling.sum()
    )
    result["ceiling_height"] = ceiling.mask(invalid_ceiling)

    return result, report


def clean_dataset(
    frame,
    min_price_per_m2=1000.0,
    max_price_per_m2=10_000_000.0,
    area_tolerance=0.1,
    max_ceiling_height=10.0,
):
    """Возвращает очищенные данные, исключённые строки и отчёт."""
    if not 0 < min_price_per_m2 < max_price_per_m2:
        raise ValueError(
            "Некорректные границы цены за квадратный метр."
        )

    if area_tolerance < 0 or max_ceiling_height <= 0:
        raise ValueError("Некорректные параметры очистки признаков.")

    missing = set(DATASET_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Отсутствуют колонки: {sorted(missing)}")

    # Выбираем только исходные колонки: диагностические не попадут в результат.
    original = (
        frame[DATASET_COLUMNS]
        .copy()
        .reset_index(drop=True)
    )

    work, exact_duplicates = remove_exact_duplicates(original)

    total = work["total_area"].astype("float64")
    price = work["price"].astype("float64")

    total_ok = np.isfinite(total) & total.gt(0)
    price_ok = np.isfinite(price) & price.gt(0)

    # Используется только для контроля качества, не как признак модели.
    price_per_m2 = price / total.where(total_ok)

    reasons = pd.Series("", index=work.index, dtype="object")

    reasons.loc[~total_ok] = "invalid_total_area"
    reasons.loc[total_ok & ~price_ok] = "invalid_price"

    reasons.loc[
        total_ok & price_ok & price_per_m2.lt(min_price_per_m2)
    ] = "price_per_m2_too_low"

    reasons.loc[
        total_ok & price_ok & price_per_m2.gt(max_price_per_m2)
    ] = "price_per_m2_too_high"

    excluded = work.loc[reasons.ne("")].copy()
    excluded["reject_reason"] = reasons.loc[reasons.ne("")]

    retained = work.loc[reasons.eq("")].copy()

    cleaned, feature_report = clean_feature_values(
        retained,
        area_tolerance=area_tolerance,
        max_ceiling_height=max_ceiling_height,
    )

    rejected = pd.concat(
        [exact_duplicates, excluded],
        ignore_index=True,
    )
    cleaned = cleaned.reset_index(drop=True)

    report = {
        "input_rows": int(len(original)),
        "exact_duplicates_removed": int(len(exact_duplicates)),
        "invalid_total_area_rows": int(
            reasons.eq("invalid_total_area").sum()
        ),
        "invalid_price_rows": int(
            reasons.eq("invalid_price").sum()
        ),
        "price_per_m2_too_low_rows": int(
            reasons.eq("price_per_m2_too_low").sum()
        ),
        "price_per_m2_too_high_rows": int(
            reasons.eq("price_per_m2_too_high").sum()
        ),
        **feature_report,
        "rejected_rows": int(len(rejected)),
        "output_rows": int(len(cleaned)),
    }

    if len(cleaned) + len(rejected) != len(original):
        raise RuntimeError("Нарушен баланс строк при очистке.")

    return cleaned, rejected, report
