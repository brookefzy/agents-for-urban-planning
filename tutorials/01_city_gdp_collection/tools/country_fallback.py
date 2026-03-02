"""Country-level fallback helpers (GDP per-capita)."""

from __future__ import annotations

import requests

_COUNTRY_CODE = {
    "argentina": "AR",
    "australia": "AU",
    "austria": "AT",
    "bangladesh": "BD",
    "belgium": "BE",
    "bolivia": "BO",
    "botswana": "BW",
    "brazil": "BR",
    "canada": "CA",
    "china": "CN",
    "france": "FR",
    "germany": "DE",
    "india": "IN",
    "japan": "JP",
    "mexico": "MX",
    "united kingdom": "GB",
    "united states": "US",
}


def get_country_gdp_per_capita_usd(country: str) -> tuple[float | None, int | None, str]:
    code = _COUNTRY_CODE.get((country or "").strip().lower())
    if not code:
        return None, None, ""
    url = (
        f"https://api.worldbank.org/v2/country/{code}/indicator/NY.GDP.PCAP.CD"
        "?format=json&per_page=60"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or len(data) < 2:
            return None, None, url
        rows = data[1] or []
        for row in rows:
            val = row.get("value")
            year = row.get("date")
            if val is None:
                continue
            try:
                return float(val), int(year), url
            except Exception:
                continue
        return None, None, url
    except Exception:
        return None, None, url
