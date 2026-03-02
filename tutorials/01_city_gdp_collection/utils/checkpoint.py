"""Checkpoint helpers to skip previously processed city-country pairs."""

import pandas as pd


def load_processed_pairs(candidate_csv_path: str) -> set[tuple[str, str]]:
    try:
        df = pd.read_csv(candidate_csv_path)
    except FileNotFoundError:
        return set()

    required = {"city", "country"}
    if not required.issubset(df.columns):
        return set()

    return set((str(r.city), str(r.country)) for r in df[["city", "country"]].itertuples(index=False))
