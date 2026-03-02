"""Ranking helpers for pre-fetch candidate selection."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

_TIER_SCORE = {
    "tier1_official": 3.0,
    "tier2_academic": 2.0,
    "tier3_general": 1.0,
}

_KEYWORDS = ("gdp", "grp", "metropolitan", "metro", "city proper")
_DOMAIN_BOOST = {
    "ceicdata.com": 1.5,
    "worldbank.org": 1.0,
    "oecd.org": 1.0,
    "ec.europa.eu": 1.0,
}

_SOCIAL_DOMAIN_TERMS = (
    "x.com",
    "twitter.com",
    "instagram.com",
    "reddit.com",
    "threads.net",
    "threads.com",
    "tiktok.com",
)


def _snippet_relevance(snippet: str) -> float:
    text = (snippet or "").lower()
    return float(sum(1 for k in _KEYWORDS if k in text))


def _domain_boost(domain: str) -> float:
    d = (domain or "").lower().strip()
    for key, score in _DOMAIN_BOOST.items():
        if d == key or d.endswith(f".{key}"):
            return score
    return 0.0


def _geo_penalty(city: str, country: str, title: str, snippet: str) -> float:
    city_l = (city or "").lower().strip()
    country_l = (country or "").lower().strip()
    text = f"{title or ''} {snippet or ''}".lower()
    if not text:
        return 0.0
    # Penalize likely country-level pages that don't mention the target city.
    if country_l and country_l in text and city_l and city_l not in text:
        return -1.0
    return 0.0


def _path_topic_adjustment(source_url: str, title: str, snippet: str) -> float:
    text = f"{source_url or ''} {title or ''} {snippet or ''}".lower()
    boost_terms = (
        "gdp-by-region",
        "gdp by region",
        "gross-domestic-product",
        "metropolitan gdp",
        "gross metropolitan product",
    )
    noisy_terms = (
        "throughput",
        "house-price",
        "house price",
        "office-average-rent",
        "average-rent",
        "building-registration-area",
        "developed-area",
        "industrial-zone",
        "share-of-gdp",
    )
    score = 0.0
    if any(t in text for t in boost_terms):
        score += 2.0
    if any(t in text for t in noisy_terms):
        score -= 2.5
    return score


def prefetch_confidence_and_reasons(
    *,
    city: str,
    country: str,
    source_url: str,
    title: str,
    snippet: str,
    source_tier_label: str,
    query: str = "",
) -> tuple[float, str]:
    _ = query  # do not score using query text to avoid self-confirming relevance.
    text = f"{title or ''} {snippet or ''} {source_url or ''}".lower()
    city_l = (city or "").strip().lower()
    country_l = (country or "").strip().lower()
    reasons: list[str] = []
    score = 0.0

    if city_l and city_l in text:
        score += 2.0
        reasons.append("city_match")
    else:
        reasons.append("city_missing")
        score -= 1.0

    gdp_signal_terms = (
        "gdp",
        "grp",
        "gross domestic product",
        "gross metropolitan product",
        "gva",
        "regional accounts",
    )
    gdp_hits = sum(1 for t in gdp_signal_terms if t in text)
    if gdp_hits > 0:
        score += min(2.0, 0.5 * gdp_hits)
        reasons.append("gdp_signal")
    else:
        reasons.append("gdp_signal_missing")
        score -= 1.5

    if any(d in text for d in _SOCIAL_DOMAIN_TERMS):
        score -= 3.0
        reasons.append("social_domain_penalty")

    if country_l and country_l in text and city_l and city_l not in text:
        score -= 1.5
        reasons.append("country_level_penalty")

    tier_score = _TIER_SCORE.get((source_tier_label or "").strip(), 0.0)
    if tier_score > 0:
        score += 0.4 * tier_score
        reasons.append("source_tier_bonus")

    score += _path_topic_adjustment(source_url, title, snippet)
    if _path_topic_adjustment(source_url, title, snippet) > 0:
        reasons.append("path_gdp_bonus")
    elif _path_topic_adjustment(source_url, title, snippet) < 0:
        reasons.append("path_noisy_topic_penalty")

    confidence = max(0.0, min(1.0, (score + 5.0) / 10.0))
    return confidence, ";".join(reasons) if reasons else "none"


def rank_prefetch_candidates(candidates: Iterable[dict], top_k: int = 5) -> pd.DataFrame:
    """
    Dedupe by source_url and rank with simple deterministic signals:
    source tier + snippet relevance.
    """
    df = pd.DataFrame(list(candidates))
    if df.empty:
        return pd.DataFrame()

    if "source_url" not in df.columns:
        raise ValueError("rank_prefetch_candidates requires `source_url`")

    if "source_tier_label" not in df.columns:
        df["source_tier_label"] = "tier3_general"
    if "snippet" not in df.columns:
        df["snippet"] = ""
    if "title" not in df.columns:
        df["title"] = ""
    if "source_domain" not in df.columns:
        df["source_domain"] = ""
    if "city" not in df.columns:
        df["city"] = ""
    if "country" not in df.columns:
        df["country"] = ""

    df = df.drop_duplicates(subset=["source_url"], keep="first").copy()
    df["tier_score"] = df["source_tier_label"].map(_TIER_SCORE).fillna(0.0)
    df["snippet_score"] = df["snippet"].astype(str).map(_snippet_relevance)
    df["domain_boost"] = df["source_domain"].astype(str).map(_domain_boost)
    df["geo_penalty"] = df.apply(
        lambda r: _geo_penalty(
            city=str(r.get("city") or ""),
            country=str(r.get("country") or ""),
            title=str(r.get("title") or ""),
            snippet=str(r.get("snippet") or ""),
        ),
        axis=1,
    )
    df["path_topic_adjustment"] = df.apply(
        lambda r: _path_topic_adjustment(
            source_url=str(r.get("source_url") or ""),
            title=str(r.get("title") or ""),
            snippet=str(r.get("snippet") or ""),
        ),
        axis=1,
    )
    df[["prefetch_confidence", "prefetch_reasons"]] = df.apply(
        lambda r: pd.Series(
            prefetch_confidence_and_reasons(
                city=str(r.get("city") or ""),
                country=str(r.get("country") or ""),
                source_url=str(r.get("source_url") or ""),
                title=str(r.get("title") or ""),
                snippet=str(r.get("snippet") or ""),
                source_tier_label=str(r.get("source_tier_label") or ""),
                query=str(r.get("query") or ""),
            )
        ),
        axis=1,
    )
    df["prefetch_rank_score"] = (
        df["tier_score"] * 10.0
        + df["snippet_score"]
        + df["domain_boost"]
        + df["geo_penalty"]
        + df["path_topic_adjustment"]
        + df["prefetch_confidence"] * 3.0
    )
    df = df.sort_values(by="prefetch_rank_score", ascending=False, na_position="last")
    return df.head(top_k).reset_index(drop=True)


def weighted_quality_score(row: dict) -> float:
    """Provisional scoring. TODO(source_tier_mapping)."""
    tier_label = row.get("source_tier_label", "tier3_general")
    tier_score = _TIER_SCORE.get(tier_label, 0.0)
    year = row.get("year")
    try:
        year_score = float(year) / 1000.0
    except Exception:
        year_score = 0.0
    evidence = row.get("evidence_text") or ""
    evidence_score = min(len(str(evidence)) / 200.0, 1.0)
    return tier_score + year_score + evidence_score
