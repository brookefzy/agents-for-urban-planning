"""Deterministic search client wrappers (SERPAPI/TAVILY) with retry/backoff."""

from __future__ import annotations

import os

import requests

from utils.retry import retry_with_exponential_backoff


_COUNTRY_DEMONYMS: dict[str, tuple[str, ...]] = {
    "usa": ("united states", "us", "american"),
    "united states": ("usa", "us", "american"),
    "uk": ("united kingdom", "british"),
    "united kingdom": ("uk", "british"),
    "germany": ("deutschland", "german"),
    "china": ("prc", "chinese"),
}

_AMBIGUOUS_CITY_CUES: dict[str, dict[str, tuple[str, ...]]] = {
    "los angeles": {
        "positive": ("california", "la county", "metro area"),
        "negative": ("laos",),
    },
    "la": {
        "positive": ("los angeles", "california", "metro area"),
        "negative": ("laos",),
    },
    "georgia": {
        "positive": ("state", "usa"),
        "negative": ("country", "tbilisi"),
    },
}


def _country_aliases(country: str) -> tuple[str, ...]:
    c = (country or "").strip().lower()
    if not c:
        return tuple()
    aliases = [c, c.replace("&", "and"), c.replace(" ", "-")]
    aliases.extend(_COUNTRY_DEMONYMS.get(c, ()))
    seen: set[str] = set()
    ordered: list[str] = []
    for value in aliases:
        token = (value or "").strip().lower()
        if token and token not in seen:
            seen.add(token)
            ordered.append(token)
    return tuple(ordered)


def build_city_gdp_queries(city: str, country: str, year: int | None = None) -> list[str]:
    y = year or 2024
    city_s = (city or "").strip()
    country_s = (country or "").strip()
    city_l = city_s.lower()
    country_terms = _country_aliases(country_s)
    queries: list[str] = [
        f"{city} {country} GDP {y}",
        f"{city} {country} GRP nominal {y}",
        f"{city} metropolitan GDP {y}",
        f"{city} gross metropolitan product {y}",
        f"{city} {country} gross value added GVA {y}",
        f"{city} {country} regional accounts GDP {y}",
        f"site:ceicdata.com {city} {country} gdp by region {y}",
        f"\"{city_s}\" \"{country_s}\" GDP city {y}",
        f"\"{city_s}\" \"{country_s}\" gross domestic product city level {y}",
    ]
    for country_term in country_terms[:2]:
        queries.append(f"\"{city_s}\" {country_term} city GDP {y}")
    cues = _AMBIGUOUS_CITY_CUES.get(city_l)
    if cues:
        for positive in cues.get("positive", ()):
            queries.append(f"{city_s} {country_s} GDP {positive} {y}")
        negatives = " ".join(f"-{token}" for token in cues.get("negative", ()))
        if negatives:
            queries.append(f"{city_s} {country_s} GDP {y} {negatives}".strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for q in queries:
        clean = " ".join(str(q).split())
        if clean and clean not in seen:
            seen.add(clean)
            deduped.append(clean)
    return deduped


def build_city_gdp_backup_queries(city: str, country: str, year: int | None = None) -> list[str]:
    y = year or 2024
    city_s = (city or "").strip()
    country_s = (country or "").strip()
    queries = [
        f"\"{city_s}\" \"{country_s}\" GDP by region city-level {y}",
        f"\"{city_s}\" \"{country_s}\" metropolitan GDP nominal {y}",
        f"site:ceicdata.com \"{city_s}\" \"{country_s}\" gross domestic product by region {y}",
        f"\"{city_s}\" \"{country_s}\" city GDP official statistics {y}",
    ]
    deduped: list[str] = []
    seen: set[str] = set()
    for q in queries:
        clean = " ".join(str(q).split())
        if clean and clean not in seen:
            seen.add(clean)
            deduped.append(clean)
    return deduped


def build_city_gdp_recovery_queries(city: str, country: str, year: int | None = None) -> list[str]:
    y = year or 2024
    city_s = (city or "").strip()
    country_s = (country or "").strip()
    queries = [
        f"\"{city_s}\" \"{country_s}\" city GDP current prices {y}",
        f"\"{city_s}\" \"{country_s}\" metropolitan gross domestic product current prices {y}",
        f"\"{city_s}\" \"{country_s}\" regional accounts city GDP {y}",
        f"\"{city_s}\" \"{country_s}\" GDP nominal city statistics office {y}",
    ]
    deduped: list[str] = []
    seen: set[str] = set()
    for q in queries:
        clean = " ".join(str(q).split())
        if clean and clean not in seen:
            seen.add(clean)
            deduped.append(clean)
    return deduped

def _serpapi_search(query: str, top_k: int) -> list[dict]:
    key = os.getenv("SERPAPI_KEY")
    if not key:
        return [{"error": "SERPAPI_KEY missing"}]
    params = {
        "engine": "google",
        "q": query,
        "api_key": key,
        "num": max(1, min(top_k, 10)),
    }
    resp = requests.get("https://serpapi.com/search.json", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for item in data.get("organic_results", [])[:top_k]:
        rows.append(
            {
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "source": "serpapi",
            }
        )
    return rows


def _tavily_search(query: str, top_k: int) -> list[dict]:
    key = os.getenv("TAVILY_API_KEY")
    if not key:
        return [{"error": "TAVILY_API_KEY missing"}]
    resp = requests.post(
        "https://api.tavily.com/search",
        json={
            "api_key": key,
            "query": query,
            "max_results": top_k,
            "include_images": False,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for item in data.get("results", [])[:top_k]:
        rows.append(
            {
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "snippet": item.get("content", ""),
                "source": "tavily",
            }
        )
    return rows


def search_web(query: str, top_k: int = 5) -> list[dict]:
    """
    Deterministic search wrapper for a single engine.
    """
    return search_web_with_engine(query=query, engine="tavily", top_k=top_k)


def search_web_with_engine(query: str, engine: str, top_k: int = 5) -> list[dict]:
    """
    Deterministic search for an explicitly selected engine.
    Supported engines: `tavily`, `serpapi`.
    """
    normalized = (engine or "").strip().lower()
    if normalized == "tavily":
        try:
            return retry_with_exponential_backoff(
                lambda: _tavily_search(query, top_k),
                retries=2,
                retry_on_rate_limit_only=True,
            )
        except Exception as e:
            return [{"error": f"search_failed_after_retries: {e}"}]
    if normalized == "serpapi":
        try:
            return retry_with_exponential_backoff(
                lambda: _serpapi_search(query, top_k),
                retries=2,
                retry_on_rate_limit_only=True,
            )
        except Exception as e:
            return [{"error": f"search_failed_after_retries: {e}"}]
    return [{"error": f"unsupported_search_engine: {engine}"}]
