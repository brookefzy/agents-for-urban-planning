"""Workflow entrypoint for city-level GDP collection."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from typing import Callable

import pandas as pd

from agents.evaluator import EvaluatorAgent
from agents.extractor import ExtractorAgent
from agents.normalizer import NormalizerAgent
from agents.search import SearchAgent
from agents.search import validate_search_env
from tools.csv_io import write_csv
from tools.country_fallback import get_country_gdp_per_capita_usd
from tools.http_fetch import fetch_with_cache
from utils.checkpoint import load_processed_pairs
from utils.currency import expected_currency_for_country
from utils.normalization import load_city_country_inputs
from utils.ranking import rank_prefetch_candidates, weighted_quality_score
from utils.search_cache import load_search_cache, save_search_cache

CANDIDATE_COLUMNS = [
    "city",
    "country",
    "population",
    "gdp_raw",
    "year",
    "currency",
    "metric_type",
    "value_unit",
    "country_consistency",
    "repair_actions",
    "gdp_type",
    "geo_level",
    "usd_exchange_rate",
    "fx_year",
    "gdp_usd",
    "source_url",
    "search_engine",
    "source_tier",
    "source_tier_label",
    "source_domain",
    "source_organization",
    "method",
    "evidence_text",
    "evidence_path",
    "weighted_quality_score",
    "prefetch_confidence",
    "prefetch_reasons",
    "quota_stage",
    "status",
    "failure_reasons",
    "llm_used",
    "llm_attempted",
    "llm_status",
    "llm_error",
    "model_name",
]


def _tutorial_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_paths() -> dict[str, Path]:
    root = _tutorial_root()
    return {
        "cityls": root / "data/input/cityls.json",
        "city_meta": root / "data/input/city_meta.csv",
        "output_dir": root / "data/output/gdp",
    }


def _empty_candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CANDIDATE_COLUMNS)


def _ensure_candidate_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in CANDIDATE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[CANDIDATE_COLUMNS]


def _finalize_llm_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee non-null and internally consistent LLM tracking fields.
    """
    out = df.copy()
    for col in ("llm_used", "llm_attempted"):
        if col not in out.columns:
            out[col] = False
        out[col] = out[col].fillna(False).astype(bool)

    if "llm_status" not in out.columns:
        out["llm_status"] = pd.NA

    # Compute canonical status from booleans when missing/blank.
    needs_status = out["llm_status"].isna() | (out["llm_status"].astype(str).str.strip() == "")
    used_mask = out["llm_used"].astype(bool)
    attempted_mask = out["llm_attempted"].astype(bool)

    out.loc[needs_status & used_mask, "llm_status"] = "used"
    out.loc[needs_status & (~used_mask) & attempted_mask, "llm_status"] = "attempted_no_fact"
    out.loc[needs_status & (~attempted_mask), "llm_status"] = "not_attempted"
    if "llm_error" not in out.columns:
        out["llm_error"] = "none"
    out["llm_error"] = (
        out["llm_error"].fillna("none").astype(str).replace({"": "none", "nan": "none"})
    )
    if "model_name" not in out.columns:
        out["model_name"] = ""
    out["model_name"] = out["model_name"].fillna("").astype(str)
    return out


def _candidate_rows_for_dry_run(
    inputs: pd.DataFrame, *, search_engine: str
) -> pd.DataFrame:
    rows = []
    for row in inputs.itertuples(index=False):
        rows.append(
            {
                "city": row.city,
                "country": row.country,
                "population": row.population,
                "method": "direct_parser",
                "status": "pending",
                "failure_reasons": "dry_run_placeholder",
                "llm_used": False,
                "llm_attempted": False,
                "llm_status": "not_attempted",
                "llm_error": "none",
                "model_name": "",
                "country_consistency": "unknown",
                "repair_actions": "none",
                "prefetch_confidence": pd.NA,
                "prefetch_reasons": "",
                "quota_stage": "dry_run",
                "source_tier_label": "tier3_general",
                "source_tier": pd.NA,
                "search_engine": search_engine,
            }
        )
    return _ensure_candidate_schema(pd.DataFrame(rows))


def _is_high_relevance_candidate(city: str, title: str, snippet: str, query: str) -> bool:
    text = f"{title or ''} {snippet or ''} {query or ''}".lower()
    city_l = (city or "").lower()
    has_city = city_l in text if city_l else False
    has_gdp_signal = any(
        k in text
        for k in ("gdp", "grp", "gross domestic product", "gross value added", "regional accounts")
    )
    return has_city and has_gdp_signal


def _is_city_gdp_intent_candidate(
    *, city: str, title: str, snippet: str, source_url: str
) -> bool:
    text = f"{title or ''} {snippet or ''} {source_url or ''}".lower()
    blocked_domains = (
        "x.com",
        "twitter.com",
        "instagram.com",
        "reddit.com",
        "threads.net",
        "threads.com",
        "tiktok.com",
    )
    if any(d in text for d in blocked_domains):
        return False
    city_l = (city or "").strip().lower()
    if not city_l or city_l not in text:
        return False
    gdp_terms = (
        "gdp",
        "grp",
        "gross domestic product",
        "gross metropolitan product",
        "gmp",
        "gross value added",
        "gva",
        "regional accounts",
    )
    has_gdp_signal = any(term in text for term in gdp_terms)
    if not has_gdp_signal:
        return False

    negative_topic_terms = (
        "port cargo throughput",
        "throughput",
        "house price index",
        "hpi",
        "office average rent",
        "average rent",
        "building registration area",
        "building-registration-area",
        "developed area",
        "developed-area",
        "industrial zone",
        "industrial-zone",
        "commodity",
        "imports of goods and services",
        "share of gdp",
        "share-of-gdp",
    )
    if any(term in text for term in negative_topic_terms):
        return False
    return True


def _is_country_level_only_candidate(
    *, city: str, title: str, snippet: str, source_url: str
) -> bool:
    text = f"{title or ''} {snippet or ''} {source_url or ''}".lower()
    city_l = (city or "").strip().lower()
    has_city = bool(city_l and city_l in text)
    if has_city:
        return False
    country_level_markers = (
        "/indicator/ny.gdp",
        "locations=",
        "country pages",
        "country-level",
        "national gdp",
        "economic forecast",
    )
    return any(marker in text for marker in country_level_markers)


_COUNTRY_ALIAS_MAP: dict[str, set[str]] = {
    "usa": {"usa", "united states", "united-states", "u.s.", "u.s.a.", "/us/"},
    "united states": {"usa", "united states", "united-states", "u.s.", "u.s.a.", "/us/"},
    "china": {"china", "people's republic of china", "prc"},
    "germany": {"germany", "deutschland"},
    "laos": {"laos", "lao pdr", "lao"},
    "france": {"france"},
    "uk": {"uk", "united kingdom", "great britain", "britain"},
    "united kingdom": {"uk", "united kingdom", "great britain", "britain"},
    "japan": {"japan"},
    "india": {"india"},
}


def _country_aliases(country: str) -> set[str]:
    c = (country or "").strip().lower()
    if not c:
        return set()
    aliases = set(_COUNTRY_ALIAS_MAP.get(c, set()))
    aliases.add(c)
    aliases.add(c.replace("&", "and"))
    aliases.add(c.replace(" ", "-"))
    return {a for a in aliases if a}


def _contains_alias(text: str, alias: str) -> bool:
    a = (alias or "").strip().lower()
    if not a:
        return False
    if "/" in a or "-" in a or " " in a or "." in a:
        return a in text
    return re.search(rf"\b{re.escape(a)}\b", text) is not None


def _is_country_consistent_candidate(
    *, country: str, title: str, snippet: str, source_url: str
) -> bool:
    """
    Reject candidates that explicitly signal a different country than requested.
    Uses title/snippet/url only (not query text) to avoid self-confirming matches.
    """
    target_aliases = _country_aliases(country)
    if not target_aliases:
        return True
    text = f"{title or ''} {snippet or ''} {source_url or ''}".lower()
    target_hit = any(_contains_alias(text, a) for a in target_aliases)

    for country_key, aliases in _COUNTRY_ALIAS_MAP.items():
        if country_key in target_aliases or country_key == (country or "").strip().lower():
            continue
        all_aliases = set(aliases)
        all_aliases.add(country_key)
        if any(_contains_alias(text, a) for a in all_aliases):
            return target_hit
    return True


def _run_city_pipeline(
    city: str,
    country: str,
    population: float | None,
    *,
    top_k: int,
    urls_per_city_for_extraction: int,
    max_urls_to_try_per_city: int = 20,
    search_engine: str,
    cache_dir: Path,
    search_cache_dir: Path,
    use_search_cache: bool,
    allow_llm_fallback: bool,
    allow_rendered_fallback: bool = False,
    llm_research_agent_mode: bool = False,
    parser_fallback_when_llm_research_fails: bool = True,
    llm_model: str,
    llm_budget: dict,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> list[dict]:
    search_agent = SearchAgent(top_k=top_k, search_engine=search_engine)
    extractor = ExtractorAgent()
    normalizer = NormalizerAgent()
    evaluator = EvaluatorAgent()

    raw_candidates = None
    if use_search_cache:
        raw_candidates = load_search_cache(
            search_cache_dir, search_engine=search_engine, city=city, country=country
        )
    if raw_candidates is None:
        if progress_callback:
            progress_callback(
                "search_start",
                {
                    "agent": "SearchAgent",
                    "stage": "search",
                    "city": city,
                    "country": country,
                    "search_engine": search_engine,
                },
            )
        raw_candidates = search_agent.search_city(city, country, year=2024)
        for r in raw_candidates:
            if isinstance(r, dict):
                r.setdefault("quota_stage", "primary")
        # Persist only usable URL rows; avoid caching pure error payloads.
        if use_search_cache and raw_candidates and any(r.get("source_url") for r in raw_candidates):
            save_search_cache(
                search_cache_dir,
                search_engine=search_engine,
                city=city,
                country=country,
                rows=raw_candidates,
            )
    if not raw_candidates:
        if progress_callback:
            progress_callback(
                "search_empty",
                {
                    "agent": "SearchAgent",
                    "stage": "search",
                    "city": city,
                    "country": country,
                    "search_engine": search_engine,
                },
            )
        return [
            {
                "city": city,
                "country": country,
                "population": population,
                "search_engine": search_engine,
                "method": "search",
                "status": "failed",
                "failure_reasons": "search_no_results",
                "llm_used": False,
                "llm_attempted": False,
                "llm_status": "not_attempted",
                "llm_error": "none",
                "model_name": "",
            }
        ]
    if raw_candidates and raw_candidates[0].get("search_error"):
        if progress_callback:
            progress_callback(
                "search_error",
                {
                    "agent": "SearchAgent",
                    "stage": "search",
                    "city": city,
                    "country": country,
                    "search_engine": search_engine,
                    "search_error": raw_candidates[0].get("search_error"),
                },
            )
        return [
            {
                "city": city,
                "country": country,
                "population": population,
                "search_engine": search_engine,
                "method": "search",
                "status": "failed",
                "failure_reasons": f"search_error:{raw_candidates[0].get('search_error')}",
                "llm_used": False,
                "llm_attempted": False,
                "llm_status": "not_attempted",
                "llm_error": "none",
                "model_name": "",
            }
        ]
    target_valid_candidates = max(1, int(urls_per_city_for_extraction))
    max_scan = max(target_valid_candidates, int(max_urls_to_try_per_city))
    all_candidates = list(raw_candidates)

    def _intent_candidate_count(rows: list[dict]) -> tuple[int, int]:
        valid = []
        for item in rows:
            source_url = str(item.get("source_url") or "")
            if not source_url:
                continue
            if not _is_country_consistent_candidate(
                country=country,
                title=str(item.get("title") or ""),
                snippet=str(item.get("snippet") or ""),
                source_url=source_url,
            ):
                continue
            if _is_country_level_only_candidate(
                city=city,
                title=str(item.get("title") or ""),
                snippet=str(item.get("snippet") or ""),
                source_url=source_url,
            ):
                continue
            if not _is_city_gdp_intent_candidate(
                city=city,
                title=str(item.get("title") or ""),
                snippet=str(item.get("snippet") or ""),
                source_url=source_url,
            ):
                continue
            valid.append(item)
        distinct_domains = len({str(v.get("source_domain") or "").lower() for v in valid if v.get("source_domain")})
        return len(valid), distinct_domains

    seen_urls = {str(r.get("source_url") or "") for r in all_candidates if r.get("source_url")}
    quota_count, quota_domains = _intent_candidate_count(all_candidates)
    min_distinct_domains = min(2, target_valid_candidates)
    if quota_count < target_valid_candidates or quota_domains < min_distinct_domains:
        for expand_year in (2023, 2022):
            extra = search_agent.search_city(city, country, year=expand_year)
            for row in extra:
                if not isinstance(row, dict):
                    continue
                url = str(row.get("source_url") or "")
                if not url or url in seen_urls:
                    continue
                row["quota_stage"] = f"expansion_year_{expand_year}"
                all_candidates.append(row)
                seen_urls.add(url)
            quota_count, quota_domains = _intent_candidate_count(all_candidates)
            if quota_count >= target_valid_candidates and quota_domains >= min_distinct_domains:
                break

    if (
        quota_count < target_valid_candidates or quota_domains < min_distinct_domains
    ) and hasattr(search_agent, "search_city_recovery"):
        recovery_rows = search_agent.search_city_recovery(city, country, year=2024)
        for row in recovery_rows:
            if not isinstance(row, dict):
                continue
            url = str(row.get("source_url") or "")
            if not url or url in seen_urls:
                continue
            row["quota_stage"] = row.get("quota_stage") or "failure_recovery"
            all_candidates.append(row)
            seen_urls.add(url)
        quota_count, quota_domains = _intent_candidate_count(all_candidates)
        if quota_count < target_valid_candidates or quota_domains < min_distinct_domains:
            recovery_rows_older = search_agent.search_city_recovery(city, country, year=2023)
            for row in recovery_rows_older:
                if not isinstance(row, dict):
                    continue
                url = str(row.get("source_url") or "")
                if not url or url in seen_urls:
                    continue
                row["quota_stage"] = row.get("quota_stage") or "failure_recovery_2023"
                all_candidates.append(row)
                seen_urls.add(url)

    prefetch = rank_prefetch_candidates(all_candidates, top_k=max_scan)
    if "quota_stage" not in prefetch.columns:
        prefetch["quota_stage"] = "primary"

    rows: list[dict] = []
    valid_candidates_processed = 0
    for cand in prefetch.to_dict(orient="records"):
        if valid_candidates_processed >= target_valid_candidates:
            break
        source_url = cand.get("source_url")
        if not source_url:
            continue
        if not _is_country_consistent_candidate(
            country=country,
            title=str(cand.get("title") or ""),
            snippet=str(cand.get("snippet") or ""),
            source_url=str(source_url),
        ):
            rows.append(
                {
                    "city": city,
                    "country": country,
                    "population": population,
                    "source_url": source_url,
                    "search_engine": cand.get("search_engine", search_engine),
                    "source_domain": cand.get("source_domain"),
                    "source_tier": cand.get("source_tier"),
                    "source_tier_label": cand.get("source_tier_label"),
                    "method": "search",
                    "status": "failed",
                    "failure_reasons": "candidate_country_mismatch",
                    "llm_used": False,
                    "llm_attempted": False,
                    "llm_status": "not_attempted",
                    "llm_error": "none",
                    "model_name": "",
                    "country_consistency": "mismatch",
                    "repair_actions": "none",
                    "prefetch_confidence": cand.get("prefetch_confidence"),
                    "prefetch_reasons": cand.get("prefetch_reasons", ""),
                    "quota_stage": cand.get("quota_stage", "primary"),
                }
            )
            continue
        if _is_country_level_only_candidate(
            city=city,
            title=str(cand.get("title") or ""),
            snippet=str(cand.get("snippet") or ""),
            source_url=str(source_url),
        ):
            rows.append(
                {
                    "city": city,
                    "country": country,
                    "population": population,
                    "source_url": source_url,
                    "search_engine": cand.get("search_engine", search_engine),
                    "source_domain": cand.get("source_domain"),
                    "source_tier": cand.get("source_tier"),
                    "source_tier_label": cand.get("source_tier_label"),
                    "method": "search",
                    "status": "failed",
                    "failure_reasons": "candidate_country_level_only",
                    "llm_used": False,
                    "llm_attempted": False,
                    "llm_status": "not_attempted",
                    "llm_error": "none",
                    "model_name": "",
                    "country_consistency": "matched",
                    "repair_actions": "none",
                    "prefetch_confidence": cand.get("prefetch_confidence"),
                    "prefetch_reasons": cand.get("prefetch_reasons", ""),
                    "quota_stage": cand.get("quota_stage", "primary"),
                }
            )
            continue
        if not _is_city_gdp_intent_candidate(
            city=city,
            title=str(cand.get("title") or ""),
            snippet=str(cand.get("snippet") or ""),
            source_url=str(source_url),
        ):
            rows.append(
                {
                    "city": city,
                    "country": country,
                    "population": population,
                    "source_url": source_url,
                    "search_engine": cand.get("search_engine", search_engine),
                    "source_domain": cand.get("source_domain"),
                    "source_tier": cand.get("source_tier"),
                    "source_tier_label": cand.get("source_tier_label"),
                    "method": "search",
                    "status": "failed",
                    "failure_reasons": "candidate_not_city_gdp_intent",
                    "llm_used": False,
                    "llm_attempted": False,
                    "llm_status": "not_attempted",
                    "llm_error": "none",
                    "model_name": "",
                    "country_consistency": "matched",
                    "repair_actions": "none",
                    "prefetch_confidence": cand.get("prefetch_confidence"),
                    "prefetch_reasons": cand.get("prefetch_reasons", ""),
                    "quota_stage": cand.get("quota_stage", "primary"),
                }
            )
            continue
        if llm_research_agent_mode and not _is_high_relevance_candidate(
            city=city,
            title=str(cand.get("title") or ""),
            snippet=str(cand.get("snippet") or ""),
            query=str(cand.get("query") or ""),
        ):
            rows.append(
                {
                    "city": city,
                    "country": country,
                    "population": population,
                    "source_url": source_url,
                    "search_engine": cand.get("search_engine", search_engine),
                    "source_domain": cand.get("source_domain"),
                    "source_tier": cand.get("source_tier"),
                    "source_tier_label": cand.get("source_tier_label"),
                    "method": "llm_research_agent",
                    "status": "failed",
                    "failure_reasons": "candidate_low_relevance_for_llm_research",
                    "llm_used": False,
                    "llm_attempted": False,
                    "llm_status": "not_attempted",
                    "llm_error": "none",
                    "model_name": "",
                    "country_consistency": "matched",
                    "repair_actions": "none",
                    "prefetch_confidence": cand.get("prefetch_confidence"),
                    "prefetch_reasons": cand.get("prefetch_reasons", ""),
                    "quota_stage": cand.get("quota_stage", "primary"),
                }
            )
            continue
        valid_candidates_processed += 1
        if progress_callback:
            progress_callback(
                "candidate_considered",
                {
                    "agent": "SearchAgent",
                    "stage": "rank_prefetch",
                    "city": city,
                    "country": country,
                    "source_url": source_url,
                    "query": cand.get("query", ""),
                    "source_tier_label": cand.get("source_tier_label", ""),
                },
            )
        fetch_error: str | None = None
        try:
            if allow_rendered_fallback:
                fetched = fetch_with_cache(
                    source_url,
                    cache_dir=cache_dir,
                    allow_rendered_fallback=True,
                )
            else:
                fetched = fetch_with_cache(source_url, cache_dir=cache_dir)
            content = fetched.get("content_text", "")
        except Exception as e:
            fetched = {"content_text": cand.get("snippet", "")}
            content = cand.get("snippet", "")
            fetch_error = str(e)

        facts: list = []
        llm_used_for_row = False
        llm_attempted = False
        llm_failure_reason: str | None = None
        chosen_method = "direct_parser"
        city_in_content = city.lower() in (content or "").lower()
        if llm_research_agent_mode:
            chosen_method = "llm_research_agent"
            if city_in_content and int(llm_budget.get("remaining", 0)) > 0:
                llm_attempted = True
                llm_budget["remaining"] = int(llm_budget["remaining"]) - 1
                llm_budget["used"] = int(llm_budget.get("used", 0)) + 1
                facts = extractor.extract_with_llm(
                    city=city, country=country, content=content, model=llm_model
                )
                if facts:
                    llm_used_for_row = True
                else:
                    llm_failure_reason = str(getattr(extractor, "last_llm_error", None) or "llm_no_fact")
                    if parser_fallback_when_llm_research_fails:
                        facts = extractor.extract(city, country, content)
                        if facts:
                            chosen_method = "llm_research_parser_fallback"
            elif not city_in_content:
                llm_failure_reason = "city_not_in_content"
            else:
                llm_failure_reason = "llm_budget_exhausted"
        else:
            facts = extractor.extract(city, country, content)
            if (
                not facts
                and allow_llm_fallback
                and _is_high_relevance_candidate(
                    city=city,
                    title=str(cand.get("title") or ""),
                    snippet=str(cand.get("snippet") or ""),
                    query=str(cand.get("query") or ""),
                )
                and city_in_content
                and int(llm_budget.get("remaining", 0)) > 0
            ):
                llm_attempted = True
                llm_budget["remaining"] = int(llm_budget["remaining"]) - 1
                llm_budget["used"] = int(llm_budget.get("used", 0)) + 1
                facts = extractor.extract_with_llm(
                    city=city, country=country, content=content, model=llm_model
                )
                if facts:
                    llm_used_for_row = True
                    chosen_method = "llm_fallback"
                else:
                    llm_failure_reason = str(getattr(extractor, "last_llm_error", None) or "llm_no_fact")
            elif not facts and allow_llm_fallback and not city_in_content:
                llm_failure_reason = "llm_skipped_city_not_in_content"
            elif (
                not facts
                and allow_llm_fallback
                and not _is_high_relevance_candidate(
                    city=city,
                    title=str(cand.get("title") or ""),
                    snippet=str(cand.get("snippet") or ""),
                    query=str(cand.get("query") or ""),
                )
            ):
                llm_failure_reason = "llm_skipped_low_relevance_candidate"

        if not facts:
            fr = "extraction_no_fact"
            if fetch_error:
                fr = f"{fr};fetch_error:{fetch_error}"
            if llm_failure_reason:
                fr = f"{fr};{llm_failure_reason}"
            failed_row = {
                "city": city,
                "country": country,
                "population": population,
                "source_url": source_url,
                "search_engine": cand.get("search_engine", search_engine),
                "source_domain": cand.get("source_domain"),
                "source_tier": cand.get("source_tier"),
                "source_tier_label": cand.get("source_tier_label"),
                "method": chosen_method,
                "status": "failed",
                "failure_reasons": fr,
                "llm_used": llm_used_for_row,
                "llm_attempted": llm_attempted,
                "llm_status": "used" if llm_used_for_row else ("attempted_no_fact" if llm_attempted else "not_attempted"),
                "llm_error": llm_failure_reason or "none",
                "model_name": llm_model if llm_attempted else "",
                "country_consistency": "matched",
                "repair_actions": "none",
                "prefetch_confidence": cand.get("prefetch_confidence"),
                "prefetch_reasons": cand.get("prefetch_reasons", ""),
                "quota_stage": cand.get("quota_stage", "primary"),
            }
            rows.append(failed_row)
            if progress_callback:
                progress_callback(
                    "candidate_failed",
                    {
                        "agent": "ExtractorAgent",
                        "stage": "extract",
                        "city": city,
                        "country": country,
                        "source_url": source_url,
                        "query": cand.get("query", ""),
                        "source_tier_label": cand.get("source_tier_label", ""),
                        "failure_reasons": fr,
                        "llm_error": llm_failure_reason or "",
                    },
                )
            continue

        for fact in facts:
            row = {
                "city": city,
                "country": country,
                "population": population,
                "gdp_raw": fact.gdp_raw,
                "year": fact.year,
                "currency": fact.currency,
                "metric_type": fact.metric_type,
                "value_unit": fact.value_unit,
                "country_consistency": "matched",
                "repair_actions": getattr(fact, "repair_actions", None) or "none",
                "gdp_type": fact.gdp_type,
                "geo_level": fact.geo_level,
                "source_url": source_url,
                "source_tier": cand.get("source_tier"),
                "source_tier_label": cand.get("source_tier_label"),
                "search_engine": cand.get("search_engine", search_engine),
                "source_domain": cand.get("source_domain"),
                "source_organization": "",
                "prefetch_confidence": cand.get("prefetch_confidence"),
                "prefetch_reasons": cand.get("prefetch_reasons", ""),
                "quota_stage": cand.get("quota_stage", "primary"),
                "method": chosen_method,
                "evidence_text": fact.evidence_text,
                "evidence_path": fact.evidence_path,
                "_source_content": content[:20000],
                "llm_used": llm_used_for_row,
                "llm_attempted": llm_attempted,
                "llm_status": "used" if llm_used_for_row else ("attempted_no_fact" if llm_attempted else "not_attempted"),
                "llm_error": llm_failure_reason or "none",
                "model_name": llm_model if llm_attempted else "",
            }
            row = normalizer.normalize_candidate(row)
            ok, reasons = evaluator.evaluate_candidate(row)
            row["status"] = row.get("status", "ok" if ok else "failed")
            if not ok and row["status"] == "ok":
                row["status"] = "failed"
            row["failure_reasons"] = ";".join(reasons)
            row["weighted_quality_score"] = weighted_quality_score(row)
            rows.append(row)
            if progress_callback:
                progress_callback(
                    "candidate_evaluated",
                    {
                        "agent": "EvaluatorAgent",
                        "stage": "evaluate",
                        "city": city,
                        "country": country,
                        "source_url": source_url,
                        "query": cand.get("query", ""),
                        "source_tier_label": cand.get("source_tier_label", ""),
                        "status": row.get("status"),
                        "method": row.get("method"),
                        "failure_reasons": row.get("failure_reasons", ""),
                        "llm_used": bool(row.get("llm_used")),
                        "llm_attempted": bool(row.get("llm_attempted")),
                        "llm_status": str(row.get("llm_status") or ""),
                        "llm_error": str(row.get("llm_error") or ""),
                    },
                )

    # If no valid city-level row passed, add country-level downscaled fallback.
    has_city_level_success = any(
        (r.get("status") == "ok")
        and str(r.get("method") or "") in {"direct_parser", "llm_fallback"}
        for r in rows
    )
    if not has_city_level_success and population not in (None, "", 0, "0"):
        pc_usd, pc_year, source = get_country_gdp_per_capita_usd(country)
        if pc_usd is not None:
            fb = {
                "city": city,
                "country": country,
                "population": population,
                "country_gdp_per_capita_usd": pc_usd,
                "year": pc_year,
                "currency": "USD",
                "source_url": source,
                "source_tier": 1,
                "source_tier_label": "tier1_official",
                "search_engine": search_engine,
                "source_domain": "worldbank.org",
                "source_organization": "World Bank",
                "method": "downscaled_fallback",
                "evidence_text": f"fallback: gdp_per_capita={pc_usd}, population={population}",
                "evidence_path": "fallback:country_gdp_per_capita*population",
                "llm_used": False,
                "llm_attempted": False,
                "llm_status": "not_attempted",
                "llm_error": "none",
                "model_name": "",
                "country_consistency": "fallback",
                "repair_actions": "none",
                "prefetch_confidence": pd.NA,
                "prefetch_reasons": "",
                "quota_stage": "fallback",
            }
            fb = normalizer.normalize_candidate(fb)
            if fb.get("gdp_raw") in (None, "") and fb.get("gdp_usd") not in (None, ""):
                fb["gdp_raw"] = fb["gdp_usd"]
                fb["currency"] = "USD"
            ok, reasons = evaluator.evaluate_candidate(fb)
            # fallback must remain inReview regardless of other checks.
            fb["status"] = "inReview"
            fb["failure_reasons"] = ";".join(reasons)
            fb["weighted_quality_score"] = weighted_quality_score(fb)
            rows.append(fb)
            if progress_callback:
                progress_callback(
                    "fallback_used",
                    {
                        "agent": "NormalizerAgent",
                        "stage": "fallback",
                        "city": city,
                        "country": country,
                        "method": "downscaled_fallback",
                    },
                )
    return rows


def _select_final_rows(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()

    work = candidates.copy()
    work["status_rank"] = (
        work["status"]
        .astype(str)
        .str.strip()
        .map({"ok": 3, "inReview": 2, "pending": 1, "failed": 0})
        .fillna(0)
    )
    work["expected_currency"] = work["country"].astype(str).map(expected_currency_for_country)
    work["currency_pref"] = 1
    expected_mask = work["expected_currency"].notna()
    cur_upper = work["currency"].astype(str).str.upper().str.strip()
    exp_upper = work["expected_currency"].astype(str).str.upper().str.strip()
    work.loc[expected_mask & (cur_upper == exp_upper), "currency_pref"] = 2
    work.loc[
        expected_mask & (cur_upper == "USD") & (exp_upper != "USD"),
        "currency_pref",
    ] = 0

    trusted_domains = {
        "ceicdata.com",
        "worldbank.org",
        "oecd.org",
        "bea.gov",
        "ec.europa.eu",
        "destatis.de",
        "imf.org",
        "ons.gov.uk",
    }
    dom = work["source_domain"].astype(str).str.lower().str.strip()
    work["structured_source_pref"] = dom.map(
        lambda d: 2 if any(d == td or d.endswith(f".{td}") for td in trusted_domains) else 1
    )
    work["year_num"] = pd.to_numeric(work["year"], errors="coerce")
    work["weighted_quality_score_num"] = pd.to_numeric(
        work["weighted_quality_score"], errors="coerce"
    )
    work["source_tier_num"] = pd.to_numeric(work["source_tier"], errors="coerce")
    work = work.sort_values(
        by=[
            "city",
            "country",
            "status_rank",
            "currency_pref",
            "structured_source_pref",
            "year_num",
            "weighted_quality_score_num",
            "source_tier_num",
        ],
        ascending=[True, True, False, False, False, False, False, False],
        na_position="last",
    )
    final_df = work.groupby(["city", "country"], as_index=False).head(1)
    return _ensure_candidate_schema(final_df[CANDIDATE_COLUMNS])


def _limit_by_distinct_city(df: pd.DataFrame, city_count: int) -> pd.DataFrame:
    """Keep rows from the first N distinct city names."""
    if city_count <= 0 or df.empty:
        return df.iloc[0:0].copy()

    selected_cities: set[str] = set()
    keep_idx: list[int] = []
    for idx, row in df.iterrows():
        city = str(row["city"])
        if city not in selected_cities:
            if len(selected_cities) >= city_count:
                continue
            selected_cities.add(city)
        keep_idx.append(idx)
    return df.loc[keep_idx].reset_index(drop=True)


def _load_existing_candidates(candidate_csv: Path) -> pd.DataFrame:
    """Load prior candidate rows for resume mode, tolerating missing/partial files."""
    if not candidate_csv.exists():
        return _empty_candidate_frame()
    try:
        df = pd.read_csv(candidate_csv)
    except Exception:
        return _empty_candidate_frame()
    return _ensure_candidate_schema(df)


def _has_meaningful_result_value(row: pd.Series) -> bool:
    for col in ("gdp_usd", "gdp_raw"):
        val = row.get(col)
        if val is None:
            continue
        try:
            if pd.isna(val):
                continue
        except Exception:
            pass
        if str(val).strip() != "":
            return True
    return False


def _load_resume_processed_pairs(existing_candidates: pd.DataFrame) -> set[tuple[str, str]]:
    """
    Resume semantics: skip only city-country pairs that already produced a usable result.
    Re-run pairs that only have failed/no-value rows.
    """
    if existing_candidates.empty:
        return set()

    processed: set[tuple[str, str]] = set()
    for _, row in existing_candidates.iterrows():
        city = str(row.get("city") or "").strip()
        country = str(row.get("country") or "").strip()
        if not city or not country:
            continue
        status = str(row.get("status") or "").strip()
        if status in {"ok", "inReview", "pending"} or _has_meaningful_result_value(row):
            processed.add((city, country))
    return processed


def run_pipeline(
    *,
    cityls_path: str | Path | None = None,
    city_meta_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    dry_run: bool = True,
    top_k: int = 5,
    urls_per_city_for_extraction: int = 5,
    max_urls_to_try_per_city: int = 20,
    limit: int | None = None,
    city_sample_size: int | None = None,
    search_engine: str = "tavily",
    fail_on_missing_search_keys: bool = True,
    allow_llm_fallback: bool = False,
    llm_research_agent_mode: bool = False,
    parser_fallback_when_llm_research_fails: bool = True,
    llm_model: str = "openai:gpt-5-nano",
    llm_max_calls: int = 20,
    llm_max_calls_per_city: int | None = None,
    allow_rendered_fallback: bool = False,
    use_checkpoint: bool | None = None,
    resume: bool = False,
    output_suffix: str | None = None,
    use_search_cache: bool = True,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """
    Run GDP workflow skeleton with normalized inputs and checkpoint skipping.

    Current scope:
    - Normalized input loading
    - Checkpoint-based skip
    - Search-engine-selectable retrieval path (`tavily` or `serpapi`)
    - Dry-run candidate/final/evaluation exports
    """
    defaults = _default_paths()
    cityls = Path(cityls_path or defaults["cityls"])
    city_meta = Path(city_meta_path or defaults["city_meta"])
    out_dir = Path(output_dir or defaults["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = (output_suffix or "").strip()
    if suffix and not suffix.startswith("_"):
        suffix = f"_{suffix}"
    candidate_csv = out_dir / f"r_city_gdp_candidates{suffix}.csv"
    results_csv = out_dir / f"city_gdp_results{suffix}.csv"
    eval_json = out_dir / f"run_evaluation{suffix}.json"

    inputs = load_city_country_inputs(cityls, city_meta)
    if limit is not None:
        inputs = inputs.head(limit)
    if city_sample_size is not None:
        inputs = _limit_by_distinct_city(inputs, city_sample_size)

    # `resume=True` means: read existing candidate output, skip already-processed city-country pairs,
    # and merge new rows back into the same output file.
    if resume:
        use_checkpoint = True
    # For sample evaluation runs, default to not using checkpoint unless explicitly requested.
    elif use_checkpoint is None:
        use_checkpoint = city_sample_size is None

    existing_candidates = _load_existing_candidates(candidate_csv) if resume else _empty_candidate_frame()
    if resume:
        processed = _load_resume_processed_pairs(existing_candidates)
    else:
        processed = load_processed_pairs(str(candidate_csv)) if use_checkpoint else set()
    if processed:
        mask = inputs.apply(
            lambda r: (str(r["city"]), str(r["country"])) not in processed, axis=1
        )
        queued = inputs[mask].reset_index(drop=True)
    else:
        queued = inputs

    search_env_status = validate_search_env(search_engine=search_engine)
    if not dry_run and fail_on_missing_search_keys and search_env_status["status"] != "ok":
        candidates = _empty_candidate_frame()
        finals = _empty_candidate_frame()
        write_csv(candidates, str(candidate_csv))
        write_csv(finals, str(results_csv))
        evaluation = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dry_run": dry_run,
            "top_k": top_k,
            "city_sample_size": city_sample_size,
            "search_engine": search_engine,
            "output_suffix": output_suffix or "",
            "urls_per_city_for_extraction": urls_per_city_for_extraction,
            "max_urls_to_try_per_city": max_urls_to_try_per_city,
            "allow_rendered_fallback": bool(allow_rendered_fallback),
            "use_search_cache": bool(use_search_cache),
            "input_rows_total": int(len(inputs)),
            "input_rows_queued": int(len(queued)),
            "skipped_via_checkpoint": int(len(inputs) - len(queued)),
            "candidate_rows_written": 0,
            "final_rows_written": 0,
            "search_env": search_env_status,
            "metrics": {"candidates_total": 0, "candidates_passed": 0, "candidates_failed": 0},
            "notes": [
                "Pipeline halted before retrieval due to missing API key for selected search engine."
            ],
        }
        eval_json.write_text(json.dumps(evaluation, indent=2), encoding="utf-8")
        return {
            "candidate_csv": str(candidate_csv),
            "results_csv": str(results_csv),
            "evaluation_json": str(eval_json),
            "input_rows_total": int(len(inputs)),
            "input_rows_queued": int(len(queued)),
            "halted": True,
        }

    candidates = _empty_candidate_frame()
    if dry_run:
        candidates = _candidate_rows_for_dry_run(queued, search_engine=search_engine)
    else:
        collected: list[dict] = []
        cache_dir = _tutorial_root() / ".tmp/fetch_cache"
        search_cache_dir = _tutorial_root() / ".tmp/search_cache"
        llm_budget = {"remaining": llm_max_calls, "used": 0}
        total_queued = int(len(queued))
        for idx, row in enumerate(queued.itertuples(index=False), start=1):
            if progress_callback:
                progress_callback(
                    "city_start",
                    {
                        "agent": "Workflow",
                        "stage": "city_start",
                        "city": row.city,
                        "country": row.country,
                        "search_engine": search_engine,
                    },
                )
            remaining_cities = max(1, total_queued - idx + 1)
            guaranteed_share = max(1, int(llm_budget["remaining"]) // remaining_cities)
            per_city_cap = llm_max_calls_per_city if llm_max_calls_per_city is not None else guaranteed_share
            city_budget_remaining = min(int(llm_budget["remaining"]), int(per_city_cap))
            city_llm_budget = {"remaining": city_budget_remaining, "used": 0}
            city_rows = _run_city_pipeline(
                row.city,
                row.country,
                row.population,
                top_k=top_k,
                urls_per_city_for_extraction=urls_per_city_for_extraction,
                max_urls_to_try_per_city=max_urls_to_try_per_city,
                search_engine=search_engine,
                cache_dir=cache_dir,
                search_cache_dir=search_cache_dir,
                use_search_cache=use_search_cache,
                allow_llm_fallback=allow_llm_fallback,
                allow_rendered_fallback=allow_rendered_fallback,
                llm_research_agent_mode=llm_research_agent_mode,
                parser_fallback_when_llm_research_fails=parser_fallback_when_llm_research_fails,
                llm_model=llm_model,
                llm_budget=city_llm_budget,
                progress_callback=progress_callback,
            )
            collected.extend(city_rows)
            llm_budget["used"] = int(llm_budget.get("used", 0)) + int(city_llm_budget.get("used", 0))
            llm_budget["remaining"] = max(
                0, int(llm_budget.get("remaining", 0)) - int(city_llm_budget.get("used", 0))
            )
            if progress_callback:
                progress_callback(
                    "city_complete",
                    {
                        "agent": "Workflow",
                        "stage": "city_complete",
                        "city": row.city,
                        "country": row.country,
                        "rows_collected": len(city_rows),
                    },
                )
        candidates = pd.DataFrame(collected) if collected else _empty_candidate_frame()
    candidates = _ensure_candidate_schema(candidates)
    candidates = _finalize_llm_fields(candidates)
    if resume and not existing_candidates.empty:
        candidates = pd.concat([existing_candidates, candidates], ignore_index=True)
        candidates = _ensure_candidate_schema(candidates).drop_duplicates().reset_index(drop=True)
        candidates = _finalize_llm_fields(candidates)
    finals = _select_final_rows(candidates)
    finals = _finalize_llm_fields(finals)
    evaluator = EvaluatorAgent()
    eval_summary = evaluator.summarize_run(
        candidates.to_dict(orient="records"), finals.to_dict(orient="records")
    )

    write_csv(candidates, str(candidate_csv))
    write_csv(finals, str(results_csv))

    evaluation = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "top_k": top_k,
        "city_sample_size": city_sample_size,
        "search_engine": search_engine,
        "output_suffix": output_suffix or "",
        "urls_per_city_for_extraction": urls_per_city_for_extraction,
        "max_urls_to_try_per_city": max_urls_to_try_per_city,
        "allow_rendered_fallback": bool(allow_rendered_fallback),
        "use_search_cache": bool(use_search_cache),
        "resume": bool(resume),
        "allow_llm_fallback": allow_llm_fallback,
        "llm_research_agent_mode": llm_research_agent_mode,
        "parser_fallback_when_llm_research_fails": parser_fallback_when_llm_research_fails,
        "llm_model": llm_model if (allow_llm_fallback or llm_research_agent_mode) else "",
        "llm_max_calls": llm_max_calls,
        "llm_max_calls_per_city": llm_max_calls_per_city,
        "input_rows_total": int(len(inputs)),
        "input_rows_queued": int(len(queued)),
        "skipped_via_checkpoint": int(len(inputs) - len(queued)),
        "use_checkpoint": bool(use_checkpoint),
        "existing_candidate_rows_loaded": int(len(existing_candidates)) if resume else 0,
        "candidate_rows_written": int(len(candidates)),
        "final_rows_written": int(len(finals)),
        "search_env": search_env_status,
        "metrics": eval_summary,
        "llm_calls_used": int(candidates["llm_used"].fillna(False).astype(bool).sum())
        if not candidates.empty
        else 0,
        "notes": [
            "Workflow skeleton active: retrieval/extraction/normalization implementation remains in-progress."
        ],
    }
    eval_json.write_text(json.dumps(evaluation, indent=2), encoding="utf-8")
    if progress_callback:
        progress_callback(
            "pipeline_complete",
            {
                "agent": "Workflow",
                "stage": "pipeline_complete",
                "candidate_rows_written": int(len(candidates)),
                "final_rows_written": int(len(finals)),
            },
        )

    return {
        "candidate_csv": str(candidate_csv),
        "results_csv": str(results_csv),
        "evaluation_json": str(eval_json),
        "input_rows_total": int(len(inputs)),
        "input_rows_queued": int(len(queued)),
    }
