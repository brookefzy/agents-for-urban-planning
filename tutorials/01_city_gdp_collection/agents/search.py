"""Search agent with provider-aware model initialization."""

from __future__ import annotations

import os
from datetime import datetime
import importlib
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from tools.search_clients import (
    build_city_gdp_backup_queries,
    build_city_gdp_queries,
    build_city_gdp_recovery_queries,
    search_web_with_engine,
)
from utils.source_tiering import extract_domain, get_source_tier

_BLOCKED_DOMAINS = {"youtube.com", "youtu.be", "facebook.com"}


def _is_blocked_domain(domain: str) -> bool:
    d = (domain or "").lower().strip()
    return any(d == b or d.endswith(f".{b}") for b in _BLOCKED_DOMAINS)


def _load_env() -> None:
    """Load root and local env files, favoring explicit environment variables."""
    root_env = Path(__file__).resolve().parents[2] / ".env"  # _scripts/.env
    local_env = (
        Path(__file__).resolve().parents[1] / ".env"
    )  # tutorials/01_city_gdp_collection/.env
    if root_env.exists():
        load_dotenv(root_env, override=False)
    if local_env.exists():
        load_dotenv(local_env, override=False)


def validate_search_env(search_engine: str | None = None) -> dict[str, str]:
    """
    Validate retrieval API key availability.
    At least one deterministic web source key should be present.
    """
    _load_env()
    has_tavily = bool(os.getenv("TAVILY_API_KEY"))
    has_serp = bool(os.getenv("SERPAPI_KEY"))

    engine = (search_engine or "").strip().lower()
    if engine == "tavily":
        return (
            {"status": "ok", "message": "tavily env ready"}
            if has_tavily
            else {
                "status": "degraded",
                "message": "Missing TAVILY_API_KEY for selected engine=tavily.",
            }
        )
    if engine == "serpapi":
        return (
            {"status": "ok", "message": "serpapi env ready"}
            if has_serp
            else {
                "status": "degraded",
                "message": "Missing SERPAPI_KEY for selected engine=serpapi.",
            }
        )
    if has_tavily or has_serp:
        return {"status": "ok", "message": "search env ready"}
    return {
        "status": "degraded",
        "message": "Missing both TAVILY_API_KEY and SERPAPI_KEY.",
    }


def _resolve_provider_model(model: str) -> tuple[str, str]:
    """
    Resolve model strings like:
    - `openai:gpt-4o`
    - `anthropic:claude-3-5-sonnet-20241022`
    - bare `gpt-*` / `claude-*`
    """
    model = model.strip()
    if ":" in model:
        provider, model_name = model.split(":", 1)
        provider = provider.lower()
    elif model.startswith("claude"):
        provider, model_name = "anthropic", model
    else:
        provider, model_name = "openai", model
    return provider, model_name


def _run_llm(provider: str, model_name: str, prompt: str) -> str:
    if provider == "openai":
        from openai import OpenAI

        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        resp = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return (resp.choices[0].message.content or "").strip()

    if provider == "anthropic":
        from anthropic import Anthropic

        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = client.messages.create(
            model=model_name,
            max_tokens=1200,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        chunks = []
        for block in resp.content:
            text = getattr(block, "text", None)
            if text:
                chunks.append(text)
        return "\n".join(chunks).strip()

    raise ValueError(f"Unsupported provider '{provider}' for model '{model_name}'.")


def _load_research_tools():
    """Import research tools lazily so optional deps do not break module import."""
    try:
        return importlib.import_module("tools.research_tools")
    except Exception:
        return importlib.import_module("research_tools")


def _safe_tool_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return [{"error": str(e)}]


def find_references(
    task: str,
    model: str = "openai:gpt-4o-mini",
    return_messages: bool = False,
    tavily_k: int = 8,
    arxiv_k: int = 5,
    allow_llm: bool = False,
) -> str | tuple[str, list[dict[str, str]]]:
    """
    Retrieve deterministic references first, then optionally synthesize via LLM.

    LLM is used only for summary/parsing; search retrieval remains deterministic.
    """
    _load_env()
    provider, model_name = _resolve_provider_model(model)
    research_tools = _load_research_tools()

    deterministic_payload: dict[str, Any] = {
        "task": task,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": {
            "tavily": _safe_tool_call(
                research_tools.tavily_search_tool, task, max_results=tavily_k
            ),
            "wikipedia": _safe_tool_call(
                research_tools.wikipedia_search_tool, task, sentences=4
            ),
            "arxiv": _safe_tool_call(
                research_tools.arxiv_search_tool, task, max_results=arxiv_k
            ),
        },
    }

    prompt = f"""
You are assisting a city-GDP data collection pipeline.
Use only the provided deterministic search payload, do not invent URLs.

Task:
{task}

Search payload:
{deterministic_payload}

Return:
1) concise synthesis
2) bullet list of candidate URLs with one-line rationale each
3) explicit caveats for geo-level mismatch (country vs metro/city)
""".strip()

    messages = [{"role": "user", "content": prompt}]

    if not allow_llm:
        deterministic_only = (
            "[LLM disabled: deterministic payload only]\n" f"{deterministic_payload}"
        )
        return (deterministic_only, messages) if return_messages else deterministic_only

    try:
        content = _run_llm(provider, model_name, prompt)
        return (content, messages) if return_messages else content
    except Exception as e:
        fallback = (
            f"[Model Error: {e}]\nDeterministic payload:\n{deterministic_payload}"
        )
        return (fallback, messages) if return_messages else fallback


class SearchAgent:
    """Deterministic search agent with query expansion and source tier annotation."""

    def __init__(self, top_k: int = 5, search_engine: str = "tavily") -> None:
        self.top_k = top_k
        self.search_engine = search_engine
        _load_env()

    def build_queries(
        self, city: str, country: str, year: int | None = None
    ) -> list[str]:
        return build_city_gdp_queries(city, country, year=year)

    def build_backup_queries(
        self, city: str, country: str, year: int | None = None
    ) -> list[str]:
        return build_city_gdp_backup_queries(city, country, year=year)

    def build_recovery_queries(
        self, city: str, country: str, year: int | None = None
    ) -> list[str]:
        return build_city_gdp_recovery_queries(city, country, year=year)

    @staticmethod
    def _has_city_gdp_signal(city: str, title: str, snippet: str, source_url: str) -> bool:
        text = f"{title or ''} {snippet or ''} {source_url or ''}".lower()
        city_l = (city or "").strip().lower()
        if not city_l or city_l not in text:
            return False
        if not any(k in text for k in ("gdp", "grp", "gross domestic product", "gva", "gmp")):
            return False
        noisy = (
            "throughput",
            "rent",
            "house-price",
            "house price",
            "building-registration-area",
            "developed-area",
            "industrial-zone",
            "share-of-gdp",
        )
        return not any(k in text for k in noisy)

    def search_city(
        self, city: str, country: str, year: int | None = None
    ) -> list[dict]:
        queries = self.build_queries(city, country, year=year)
        rows = []
        seen = set()
        errors: list[str] = []
        for query in queries:
            raw = search_web_with_engine(
                query=query, engine=self.search_engine, top_k=self.top_k
            )
            for item in raw:
                if item.get("error"):
                    errors.append(str(item.get("error")))
                    continue
                url = item.get("url", "")
                if not url:
                    continue
                if url in seen:
                    continue
                domain = extract_domain(url)
                if _is_blocked_domain(domain):
                    continue
                seen.add(url)
                tier_label, tier_num = get_source_tier(url)
                rows.append(
                    {
                        "city": city,
                        "country": country,
                        "query": query,
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                        "source_url": url,
                        "source_domain": domain,
                        "source_tier_label": tier_label,
                        "source_tier": tier_num,
                        "search_engine": self.search_engine,
                        "quota_stage": "primary",
                    }
                )
        if rows and not any(
            self._has_city_gdp_signal(
                city=city,
                title=str(r.get("title") or ""),
                snippet=str(r.get("snippet") or ""),
                source_url=str(r.get("source_url") or ""),
            )
            for r in rows
        ):
            for query in self.build_backup_queries(city, country, year=year):
                raw = search_web_with_engine(
                    query=query, engine=self.search_engine, top_k=self.top_k
                )
                for item in raw:
                    if item.get("error"):
                        errors.append(str(item.get("error")))
                        continue
                    url = item.get("url", "")
                    if not url or url in seen:
                        continue
                    domain = extract_domain(url)
                    if _is_blocked_domain(domain):
                        continue
                    seen.add(url)
                    tier_label, tier_num = get_source_tier(url)
                    rows.append(
                        {
                            "city": city,
                            "country": country,
                            "query": query,
                            "title": item.get("title", ""),
                            "snippet": item.get("snippet", ""),
                            "source_url": url,
                            "source_domain": domain,
                            "source_tier_label": tier_label,
                            "source_tier": tier_num,
                            "search_engine": self.search_engine,
                            "quota_stage": "backup_query_pack",
                        }
                    )
        if not rows and errors:
            return [
                {
                    "city": city,
                    "country": country,
                    "search_engine": self.search_engine,
                    "search_error": " | ".join(errors[:5]),
                }
            ]
        return rows

    def search_city_recovery(
        self, city: str, country: str, year: int | None = None
    ) -> list[dict]:
        queries = self.build_recovery_queries(city, country, year=year)
        rows: list[dict] = []
        seen: set[str] = set()
        for query in queries:
            raw = search_web_with_engine(
                query=query, engine=self.search_engine, top_k=self.top_k
            )
            for item in raw:
                if item.get("error"):
                    continue
                url = item.get("url", "")
                if not url or url in seen:
                    continue
                domain = extract_domain(url)
                if _is_blocked_domain(domain):
                    continue
                seen.add(url)
                tier_label, tier_num = get_source_tier(url)
                rows.append(
                    {
                        "city": city,
                        "country": country,
                        "query": query,
                        "title": item.get("title", ""),
                        "snippet": item.get("snippet", ""),
                        "source_url": url,
                        "source_domain": domain,
                        "source_tier_label": tier_label,
                        "source_tier": tier_num,
                        "search_engine": self.search_engine,
                        "quota_stage": "failure_recovery",
                    }
                )
        return rows
