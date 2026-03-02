"""Per-search-engine search result cache helpers."""

from __future__ import annotations

import json
import re
from pathlib import Path


def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "unknown"


def search_cache_path(
    cache_root: str | Path, *, search_engine: str, city: str, country: str
) -> Path:
    base = Path(cache_root) / _slug(search_engine)
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{_slug(city)}__{_slug(country)}.json"


def load_search_cache(
    cache_root: str | Path, *, search_engine: str, city: str, country: str
) -> list[dict] | None:
    path = search_cache_path(
        cache_root, search_engine=search_engine, city=city, country=country
    )
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return payload
    except Exception:
        return None
    return None


def save_search_cache(
    cache_root: str | Path,
    *,
    search_engine: str,
    city: str,
    country: str,
    rows: list[dict],
) -> Path:
    path = search_cache_path(
        cache_root, search_engine=search_engine, city=city, country=country
    )
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return path
