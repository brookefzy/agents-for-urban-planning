"""Deterministic HTTP fetch helpers with local cache support."""

from __future__ import annotations

from pathlib import Path
import re
import time
from typing import Any
from typing import Callable
from urllib.parse import unquote, urlparse

import requests

from utils.cache import cache_path_for_url

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_GDP_SIGNAL_TERMS = (
    "gross domestic product",
    "gdp",
    "grp",
    "economic output",
    "regional accounts",
)


def _mediawiki_parse_fallback(url: str, timeout: int) -> str | None:
    """Fetch Wikipedia HTML through MediaWiki API when direct page fetch is blocked (403)."""
    parsed = urlparse(url)
    if not parsed.netloc.endswith("wikipedia.org"):
        return None
    if not parsed.path.startswith("/wiki/"):
        return None

    page_title = unquote(parsed.path.split("/wiki/", 1)[1]).strip()
    if not page_title:
        return None

    api_url = f"{parsed.scheme or 'https'}://{parsed.netloc}/w/api.php"
    resp = requests.get(
        api_url,
        timeout=timeout,
        headers=_DEFAULT_HEADERS,
        params={
            "action": "parse",
            "page": page_title,
            "prop": "text",
            "format": "json",
            "formatversion": "2",
        },
    )
    resp.raise_for_status()
    payload = resp.json()
    text_payload = payload.get("parse", {}).get("text")
    if isinstance(text_payload, str):
        return text_payload or None
    if isinstance(text_payload, dict):
        html = text_payload.get("*", "")
        return html or None
    return None


def _fetch_text_and_type(url: str, timeout: int) -> tuple[str, str]:
    resp = requests.get(url, timeout=timeout, headers=_DEFAULT_HEADERS)
    try:
        resp.raise_for_status()
        return resp.text, resp.headers.get("content-type", "text/plain")
    except requests.HTTPError as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 403:
            fallback_html = _mediawiki_parse_fallback(url, timeout)
            if fallback_html is not None:
                return fallback_html, "text/html"
        raise


def _alternate_url_candidates(url: str) -> list[str]:
    parsed = urlparse(url)
    host = (parsed.netloc or "").strip()
    if not host:
        return []
    alts: list[str] = []
    if host.startswith("www."):
        alt_host = host[4:]
        if alt_host:
            alts.append(parsed._replace(netloc=alt_host).geturl())
    elif "." in host and not host.startswith("api."):
        alts.append(parsed._replace(netloc=f"www.{host}").geturl())
    return alts


def _is_retryable_fetch_error(exc: Exception) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return status in {408, 425, 429, 500, 502, 503, 504}
    txt = str(exc).lower()
    return any(
        token in txt
        for token in (
            "nameresolutionerror",
            "failed to resolve",
            "temporary failure in name resolution",
            "nodename nor servname",
            "connection reset",
        )
    )


def _fetch_with_retry_and_switch(url: str, timeout: int) -> tuple[str, str]:
    """
    Retry transient transport errors and fallback to host variant if needed.
    """
    attempts_by_url = [url] + _alternate_url_candidates(url)
    last_err: Exception | None = None
    for url_idx, candidate_url in enumerate(attempts_by_url):
        max_attempts = 3 if url_idx == 0 else 2
        for attempt in range(max_attempts):
            try:
                return _fetch_text_and_type(candidate_url, timeout)
            except Exception as exc:  # noqa: PERF203
                last_err = exc
                if attempt < max_attempts - 1 and _is_retryable_fetch_error(exc):
                    time.sleep(0.5 * (2**attempt))
                    continue
                break
    raise RuntimeError(f"fetch_exhausted:{last_err}")


def fetch_text(url: str, timeout: int = 30) -> str:
    text, _ = _fetch_text_and_type(url, timeout)
    return text


def should_use_rendered_fallback(content_text: str, content_type: str, source_url: str) -> bool:
    """
    Decide if a dynamic-render fetch should be attempted.
    Heuristic only: prefer deterministic static HTML unless the page looks JS-heavy.
    """
    text = (content_text or "").lower()
    ctype = (content_type or "").lower()
    url = (source_url or "").lower()
    has_gdp_signal = any(term in text for term in _GDP_SIGNAL_TERMS) or any(
        term in url for term in ("gdp", "gross-domestic-product")
    )
    if not has_gdp_signal:
        return False
    low_number_signal = len(re.findall(r"\b\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\b", text)) < 2
    js_placeholder_signal = any(
        marker in text
        for marker in (
            "enable javascript",
            "loading...",
            "__next",
            "window.__",
            "application/json",
            "hydration",
        )
    )
    return "html" in ctype and (low_number_signal or js_placeholder_signal)


def _fetch_rendered_text_default(url: str, timeout: int) -> str | None:
    """
    Optional rendered fetch extension point.
    Uses Playwright path only when installed and explicitly enabled by caller.
    """
    try:
        from tools.rendered_fetch import fetch_rendered_text

        return fetch_rendered_text(url, timeout=timeout)
    except Exception:
        return None


def fetch_with_cache(
    url: str,
    *,
    cache_dir: str | Path,
    timeout: int = 30,
    allow_rendered_fallback: bool = False,
    rendered_fetcher: Callable[[str, int], str | None] | None = None,
) -> dict[str, Any]:
    """
    Fetch URL and cache text body by URL hash.
    Returns metadata for auditability and cache trace.
    """
    body_path = cache_path_for_url(cache_dir, url, extension=".txt")
    if body_path.exists():
        return {
            "source_url": url,
            "cache_path": str(body_path),
            "from_cache": True,
            "content_text": body_path.read_text(encoding="utf-8"),
            "content_type": "text/plain",
            "used_rendered_fallback": False,
        }

    text, content_type = _fetch_with_retry_and_switch(url, timeout)
    used_rendered_fallback = False
    if allow_rendered_fallback and should_use_rendered_fallback(
        text, content_type, source_url=url
    ):
        rendered_body_path = cache_path_for_url(cache_dir, f"{url}#rendered", extension=".txt")
        if rendered_body_path.exists():
            text = rendered_body_path.read_text(encoding="utf-8")
            content_type = "text/html"
            used_rendered_fallback = True
        else:
            fallback = (rendered_fetcher or _fetch_rendered_text_default)(url, timeout)
            if fallback:
                text = fallback
                content_type = "text/html"
                rendered_body_path.write_text(text, encoding="utf-8")
                used_rendered_fallback = True
    body_path.write_text(text, encoding="utf-8")
    return {
        "source_url": url,
        "cache_path": str(body_path),
        "from_cache": False,
        "content_text": text,
        "content_type": content_type,
        "used_rendered_fallback": used_rendered_fallback,
    }
