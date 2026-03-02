"""Cache helpers for fetched web content."""

from __future__ import annotations

import hashlib
from pathlib import Path


def cache_key_for_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def cache_path_for_url(cache_dir: str | Path, url: str, extension: str = ".txt") -> Path:
    path = Path(cache_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{cache_key_for_url(url)}{extension}"
