"""Application helpers for Streamlit GDP collection demo."""

from __future__ import annotations

import json
import re
import statistics
from io import StringIO
from pathlib import Path
from typing import Callable

import pandas as pd

from tools.search_clients import search_web_with_engine

POP_MIN = 50_000
POP_MAX = 120_000_000


def build_sample_csv_template() -> str:
    """Return a sample CSV template for UI download."""
    return "city,country\nAustin,USA\nParis,France\n"


def load_uploaded_city_csv(file_bytes: bytes) -> pd.DataFrame:
    """Load and validate a CSV upload with required `city,country` columns."""
    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("Uploaded file must be UTF-8 encoded CSV.") from exc

    try:
        df = pd.read_csv(StringIO(text), dtype=str)
    except Exception as exc:
        raise ValueError("Unable to parse CSV file.") from exc

    if not {"city", "country"}.issubset(df.columns):
        raise ValueError("CSV must contain columns: city,country")

    work = df[["city", "country"]].copy()
    work["city"] = work["city"].fillna("").astype(str).str.strip()
    work["country"] = work["country"].fillna("").astype(str).str.strip()
    work = work[(work["city"] != "") & (work["country"] != "")]
    work = work.drop_duplicates(subset=["city", "country"], keep="first")
    return work.reset_index(drop=True)


def limit_cities_for_demo(df: pd.DataFrame, max_cities: int = 5) -> tuple[pd.DataFrame, bool]:
    """Cap rows to the first max_cities cities for demo use."""
    if len(df) <= max_cities:
        return df.copy(), False
    return df.head(max_cities).reset_index(drop=True), True


def parse_population_from_text(text: str) -> float | None:
    """Extract a plausible city population from free text snippets."""
    if not text:
        return None

    unit_map = {
        "million": 1_000_000,
        "m": 1_000_000,
        "billion": 1_000_000_000,
        "bn": 1_000_000_000,
        "thousand": 1_000,
        "k": 1_000,
    }

    pattern = re.compile(
        r"(?P<num>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
        r"\s*(?P<unit>million|billion|thousand|bn|m|k)?",
        re.IGNORECASE,
    )

    candidates: list[float] = []
    lower = text.lower()
    for match in pattern.finditer(text):
        raw_num = match.group("num")
        raw_unit = (match.group("unit") or "").lower()
        try:
            value = float(raw_num.replace(",", ""))
        except ValueError:
            continue
        # Skip likely year tokens unless explicitly scaled.
        if raw_unit == "" and value.is_integer() and 1900 <= int(value) <= 2100:
            continue
        # Keep only numeric mentions in population-like context.
        start, end = match.span()
        window = lower[max(0, start - 32) : min(len(lower), end + 32)]
        if not any(k in window for k in ("population", "residents", "inhabitants", "people", "metro")):
            continue
        multiplier = unit_map.get(raw_unit, 1)
        pop = value * multiplier
        if POP_MIN <= pop <= POP_MAX:
            candidates.append(pop)

    if not candidates:
        return None
    return max(candidates)


def infer_population_from_web(
    city: str,
    country: str,
    *,
    search_engine: str = "tavily",
    top_k: int = 5,
) -> float | None:
    """Infer a city/metro population via deterministic web search snippets."""
    query = f"{city} {country} metropolitan population"
    rows = search_web_with_engine(query=query, engine=search_engine, top_k=top_k)
    if not rows:
        return None

    values: list[float] = []
    for row in rows:
        if row.get("error"):
            continue
        text = " ".join(
            [
                str(row.get("title") or ""),
                str(row.get("snippet") or ""),
                str(row.get("content") or ""),
            ]
        )
        parsed = parse_population_from_text(text)
        if parsed is not None:
            values.append(parsed)

    if not values:
        return None
    # Median is more robust than max for noisy web snippets.
    return float(statistics.median(values))


def build_city_inputs_with_population(
    cities_df: pd.DataFrame,
    *,
    search_engine: str = "tavily",
    logger: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """Build `city,country,population` rows using web search population lookup."""
    work = cities_df.copy()
    work["population"] = pd.NA

    for idx, row in work.iterrows():
        city = str(row["city"])
        country = str(row["country"])
        if logger:
            logger(f"Population lookup [{idx + 1}/{len(work)}]: {city}, {country}")
        pop = infer_population_from_web(city, country, search_engine=search_engine)
        if pop is not None:
            work.at[idx, "population"] = int(pop)
            if logger:
                logger(f"Population found for {city}: {int(pop):,}")
        elif logger:
            logger(f"Population not found for {city}; pipeline fallback may apply.")

    return work


def write_pipeline_input_files(input_df: pd.DataFrame, work_dir: Path) -> tuple[Path, Path]:
    """Write temporary city inputs in the format expected by run_pipeline."""
    work_dir.mkdir(parents=True, exist_ok=True)

    cityls_path = work_dir / "cityls_upload.json"
    city_meta_path = work_dir / "city_meta_upload.csv"

    city_records = input_df[["city", "country"]].to_dict(orient="records")
    cityls_path.write_text(json.dumps(city_records, ensure_ascii=False, indent=2), encoding="utf-8")

    meta_df = input_df.rename(columns={"city": "City", "country": "Country", "population": "urban_pop"})[
        ["City", "Country", "urban_pop"]
    ]
    meta_df.to_csv(city_meta_path, index=False)
    return cityls_path, city_meta_path
