"""Historical FX rate helpers keyed by year."""

from __future__ import annotations

import re

from tools.search_clients import search_web_with_engine
from utils.currency import normalize_currency_code

# Static deterministic rates; used in tutorial mode without external FX API calls.
_FX_TO_USD_BY_CCY_YEAR = {
    "USD": {2020: 1.0, 2021: 1.0, 2022: 1.0, 2023: 1.0, 2024: 1.0},
    "EUR": {2020: 1.14, 2021: 1.18, 2022: 1.05, 2023: 1.08, 2024: 1.08},
    "GBP": {2020: 1.28, 2021: 1.38, 2022: 1.24, 2023: 1.24, 2024: 1.27},
    "JPY": {2020: 0.0094, 2021: 0.0091, 2022: 0.0076, 2023: 0.0071, 2024: 0.0068},
    "CNY": {2020: 0.145, 2021: 0.155, 2022: 0.148, 2023: 0.141, 2024: 0.139},
}
_WEB_FX_CACHE: dict[tuple[str, int, str], float | None] = {}


def _parse_rate_from_text(currency: str, text: str) -> float | None:
    cur = normalize_currency_code(currency)
    if not text or not cur:
        return None
    t = text.upper().replace(",", "")

    # Pattern: 1 CUR = X USD
    direct = re.search(rf"\b1\s*{re.escape(cur)}\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*USD\b", t)
    if direct:
        val = float(direct.group(1))
        if 0 < val < 1000:
            return val

    # Pattern: 1 USD = X CUR  -> inverse
    inverse = re.search(rf"\b1\s*USD\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*{re.escape(cur)}\b", t)
    if inverse:
        denom = float(inverse.group(1))
        if denom > 0:
            val = 1.0 / denom
            if 0 < val < 1000:
                return val
    return None


def _get_fx_rate_from_web(currency: str, year: int, search_engine: str = "tavily") -> float | None:
    cur = normalize_currency_code(currency)
    y = int(year)
    if not cur:
        return None
    cache_key = (cur, y, search_engine)
    if cache_key in _WEB_FX_CACHE:
        return _WEB_FX_CACHE[cache_key]

    query = f"average exchange rate {y} 1 {cur} to USD"
    rows = search_web_with_engine(query=query, engine=search_engine, top_k=5)
    if not rows:
        _WEB_FX_CACHE[cache_key] = None
        return None

    candidates: list[float] = []
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
        parsed = _parse_rate_from_text(cur, text)
        if parsed is not None:
            candidates.append(parsed)

    if not candidates:
        _WEB_FX_CACHE[cache_key] = None
        return None

    # Prefer robust middle estimate when multiple snippets disagree.
    candidates.sort()
    rate = candidates[len(candidates) // 2]
    _WEB_FX_CACHE[cache_key] = rate
    return rate


def get_historical_fx_rate(
    currency: str, year: int, *, allow_web_fallback: bool = True, search_engine: str = "tavily"
) -> float | None:
    cur = normalize_currency_code(currency)
    if not cur:
        return None
    y = int(year)
    by_year = _FX_TO_USD_BY_CCY_YEAR.get(cur)
    if by_year:
        if y in by_year:
            return by_year[y]
        nearest_year = min(by_year.keys(), key=lambda k: abs(k - y))
        return by_year[nearest_year]
    if allow_web_fallback:
        return _get_fx_rate_from_web(cur, y, search_engine=search_engine)
    return None
