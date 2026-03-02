"""Input normalization helpers for city-country pairs."""

import json
from pathlib import Path

import pandas as pd


def _pick_first_nonempty(record: dict, keys: list[str]) -> str | None:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        s = str(value).strip()
        if s:
            return s
    return None


def normalize_city_country_pairs(city_records: list[dict]) -> pd.DataFrame:
    """
    Normalize heterogenous city records to canonical columns:
    `city`, `country`.
    """
    normalized = []
    for row in city_records:
        city = _pick_first_nonempty(row, ["city", "City", "city_name", "name"])
        country = _pick_first_nonempty(
            row,
            ["country", "Country", "country_clean", "country_name", "nation"],
        )
        if city and country:
            normalized.append({"city": city, "country": country})

    df = pd.DataFrame(normalized)
    if df.empty:
        return pd.DataFrame(columns=["city", "country"])

    return df.drop_duplicates(subset=["city", "country"], keep="first").reset_index(
        drop=True
    )


def load_city_country_inputs(
    cityls_path: str | Path, city_meta_path: str | Path
) -> pd.DataFrame:
    """
    Load and normalize input city pairs, and attach `population` from `urban_pop`.
    """
    cityls_path = Path(cityls_path)
    city_meta_path = Path(city_meta_path)

    with cityls_path.open("r", encoding="utf-8") as f:
        raw_cityls = json.load(f)
    city_pairs = normalize_city_country_pairs(raw_cityls)

    meta = pd.read_csv(city_meta_path)
    meta_city = (
        meta.rename(
            columns={"City": "city", "city": "city", "Country": "country", "country": "country"}
        )
        .copy()
    )
    cols = [c for c in ["city", "country", "urban_pop"] if c in meta_city.columns]
    meta_city = meta_city[cols]
    if "urban_pop" in meta_city.columns:
        meta_city = meta_city.rename(columns={"urban_pop": "population"})
    else:
        meta_city["population"] = pd.NA

    meta_city = meta_city.drop_duplicates(subset=["city", "country"], keep="first")
    out = city_pairs.merge(meta_city, on=["city", "country"], how="left")

    # Coerce population values such as "3,120,612" into numeric.
    out["population"] = (
        out["population"]
        .astype("string")
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"<NA>": pd.NA, "": pd.NA, "nan": pd.NA})
    )
    out["population"] = pd.to_numeric(out["population"], errors="coerce")
    return out
