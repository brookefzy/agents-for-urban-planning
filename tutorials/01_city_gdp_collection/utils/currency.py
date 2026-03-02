"""Currency inference helpers."""

from __future__ import annotations

import re

_SYMBOL_TO_CURRENCY = {
    "€": "EUR",
    "£": "GBP",
    "¥": "JPY",
    "₹": "INR",
    "$": "USD",
}

_COUNTRY_TO_CURRENCY = {
    "argentina": "ARS",
    "australia": "AUD",
    "austria": "EUR",
    "bangladesh": "BDT",
    "belgium": "EUR",
    "brazil": "BRL",
    "canada": "CAD",
    "china": "CNY",
    "eurozone": "EUR",
    "france": "EUR",
    "germany": "EUR",
    "india": "INR",
    "indonesia": "IDR",
    "japan": "JPY",
    "mexico": "MXN",
    "new zealand": "NZD",
    "switzerland": "CHF",
    "sweden": "SEK",
    "norway": "NOK",
    "denmark": "DKK",
    "united kingdom": "GBP",
    "uk": "GBP",
    "united states": "USD",
    "usa": "USD",
}

_CURRENCY_ALIASES = {
    "$": "USD",
    "US$": "USD",
    "RMB": "CNY",
    "CNH": "CNY",
    "YUAN": "CNY",
    "RENMINBI": "CNY",
    "￥": "CNY",
}


def expected_currency_for_country(country: str | None) -> str | None:
    c = (country or "").strip().lower()
    if not c:
        return None
    return _COUNTRY_TO_CURRENCY.get(c)


def normalize_currency_code(raw: str | None) -> str | None:
    token = (raw or "").strip().upper()
    if not token:
        return None
    return _CURRENCY_ALIASES.get(token, token)


def guess_currency(text: str | None, country: str | None = None) -> str | None:
    """Best-effort currency guess from text symbols/codes and country context."""
    t = (text or "").strip()
    low = t.lower()

    # Prefer explicit 3-letter currency codes.
    code_match = re.search(
        r"\b(USD|EUR|GBP|JPY|INR|CNY|CNH|RMB|AUD|CAD|BRL|MXN|ARS|IDR)\b",
        t,
    )
    if code_match:
        return normalize_currency_code(code_match.group(1))

    c = (country or "").strip().lower()
    for symbol, code in _SYMBOL_TO_CURRENCY.items():
        if symbol in t:
            # "¥" is ambiguous (JPY/CNY); use country context to disambiguate.
            if symbol == "¥":
                if c == "china":
                    return "CNY"
                if c == "japan":
                    return "JPY"
            return code

    if any(k in low for k in (" renminbi", " rmb", " yuan", " chinese yuan")):
        return "CNY"

    if c in _COUNTRY_TO_CURRENCY:
        return _COUNTRY_TO_CURRENCY[c]

    # Country hints embedded in text.
    for k, code in _COUNTRY_TO_CURRENCY.items():
        if k in low:
            return code
    return None
