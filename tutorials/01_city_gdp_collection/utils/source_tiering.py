"""Source tier labeling helpers for GDP search ranking."""

from __future__ import annotations

from urllib.parse import urlparse

TIER1_DOMAINS = {
    "oecd.org",
    "bea.gov",
    "ec.europa.eu",
    "worldbank.org",
    "imf.org",
    "ons.gov.uk",
    "stats.gov",
}

TIER2_DOMAINS = {
    "brookings.edu",
    "oxfordeconomics.com",
    "nber.org",
    "ceicdata.com",
}


def extract_domain(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def get_source_tier(url: str) -> tuple[str, int | None]:
    """
    Return (source_tier_label, source_tier_numeric_placeholder).
    Numeric mapping is provisional. TODO(source_tier_mapping)
    """
    domain = extract_domain(url)
    if any(domain.endswith(d) for d in TIER1_DOMAINS):
        return "tier1_official", 1
    if any(domain.endswith(d) for d in TIER2_DOMAINS):
        return "tier2_academic", 2
    return "tier3_general", 3
