"""Reference/link extraction and quality evaluation helpers."""

import json
import re
import urllib.parse
from typing import Any

_URL_RE = re.compile(r"https?://[^\s\)\]\}<>\"']+", re.IGNORECASE)


def _extract_hostname(url: str) -> str:
    try:
        host = urllib.parse.urlparse(url).hostname or ""
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def extract_urls(text: str) -> list[dict[str, Any]]:
    """Best-effort URL extractor from arbitrary text."""
    if not isinstance(text, str):
        text = str(text)
    urls = _URL_RE.findall(text)
    items = []
    for url in urls:
        host = _extract_hostname(url)
        items.append({"title": None, "url": url, "source": host or None})
    return items


def evaluate_anytext_against_domains(
    top_domains: set[str], payload: Any, min_ratio: float = 0.4
) -> tuple[bool, dict[str, Any]]:
    """
    Accept payload as list[dict], dict-with-results, or free text;
    return evaluation report against trusted domains.
    """
    items = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict) and isinstance(payload.get("results"), list):
        items = payload["results"]
    elif isinstance(payload, str):
        s = payload.strip()
        if s.startswith("```"):
            s = re.sub(r"^```(?:json|text|markdown)?\s*", "", s)
            s = re.sub(r"\s*```$", "", s)
        try:
            maybe = json.loads(s)
            items = maybe if isinstance(maybe, list) else extract_urls(payload)
        except Exception:
            items = extract_urls(payload)
    else:
        items = extract_urls(str(payload))

    total = len(items)
    if total == 0:
        return False, {
            "total": 0,
            "approved": 0,
            "ratio": 0.0,
            "details": [],
            "note": "No items/links parsed",
        }

    approved = 0
    details = []
    for item in items:
        url = (item or {}).get("url")
        host = _extract_hostname(url or "")
        ok = any(host.endswith(dom) for dom in top_domains) if host else False
        if ok:
            approved += 1
        details.append(
            {
                "title": (item or {}).get("title"),
                "url": url,
                "host": host,
                "approved": ok,
            }
        )

    ratio = approved / max(total, 1)
    return ratio >= min_ratio, {
        "total": total,
        "approved": approved,
        "ratio": ratio,
        "details": details,
        "min_ratio": min_ratio,
    }


def evaluate_references(
    history: list[tuple[str, str, str]], top_domains: set[str], min_ratio: float = 0.4
) -> str:
    """
    Evaluate the latest research-agent output against trusted domains.
    Returns a markdown report.
    """
    payload = None
    for _, agent, output in reversed(history):
        if agent == "research_agent":
            payload = output
            break

    if payload is None:
        for _, _, output in reversed(history):
            if isinstance(output, str) and (
                "http://" in output or "https://" in output
            ):
                payload = output
                break

    if payload is None:
        ok, report = False, {
            "total": 0,
            "approved": 0,
            "ratio": 0.0,
            "details": [],
            "min_ratio": min_ratio,
        }
    else:
        ok, report = evaluate_anytext_against_domains(
            top_domains, payload, min_ratio=min_ratio
        )

    status = "PASS" if ok else "FAIL"
    summary = (
        f"### Evaluation — Tavily Top Domains ({status})\n"
        f"- Total: {report['total']}\n"
        f"- Approved: {report['approved']}\n"
        f"- Ratio: {report['ratio']:.0%} (min {int(min_ratio * 100)}%)\n"
    )
    rows = ["| Host | Approved | Title |", "|---|:---:|---|"]
    for row in (report.get("details") or [])[:10]:
        rows.append(
            f"| {row.get('host') or '-'} | {'Y' if row.get('approved') else 'N'} | "
            f"{row.get('title') or row.get('url') or '-'} |"
        )
    return "\n".join([summary, *rows])


def evaluate_tavily_results(
    top_domains: set[str], raw: Any, min_ratio: float = 0.4
) -> tuple[bool, str]:
    """Compatibility wrapper returning (flag, markdown_report)."""
    ok, report = evaluate_anytext_against_domains(
        top_domains, raw, min_ratio=min_ratio
    )
    summary = (
        "### Evaluation — Tavily Top Domains\n"
        f"- Total results: {report['total']}\n"
        f"- Trusted results: {report['approved']}\n"
        f"- Ratio: {report['ratio']:.2%}\n"
        f"- Threshold: {min_ratio:.0%}\n"
        f"- Status: {'PASS' if ok else 'FAIL'}\n"
    )
    return ok, summary
