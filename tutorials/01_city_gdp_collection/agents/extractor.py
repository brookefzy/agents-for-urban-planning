"""Extractor agent: parse fetched content into GDP candidate facts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
import os
from pathlib import Path
import requests

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from utils.currency import guess_currency, normalize_currency_code
from utils.retry import retry_with_exponential_backoff
from utils.config import GDPConfig


@dataclass
class ExtractedGDPFact:
    city: str
    country: str
    gdp_raw: float | None
    year: int | None
    currency: str | None
    gdp_type: str | None
    geo_level: str | None
    evidence_text: str | None
    evidence_path: str | None
    metric_type: str | None = None
    value_unit: str | None = None
    repair_actions: str | None = None


@dataclass
class _Candidate:
    fact: ExtractedGDPFact
    confidence: float


_GDP_PATTERNS = [
    re.compile(
        r"(?P<currency>USD|EUR|GBP|JPY|\$)\s*(?P<value>\d[\d,\.]*)\s*(?P<unit>billion|million|trillion|[mbt])?",
        re.IGNORECASE,
    ),
    re.compile(
        r"GDP[^\n]{0,80}?(?P<value>\d[\d,\.]*)\s*(?P<unit>billion|million|trillion)",
        re.IGNORECASE,
    ),
]

_CFG = GDPConfig()


def _parse_years(text: str) -> list[int]:
    years = re.findall(r"\b(19\d{2}|20\d{2})\b", text or "")
    ys = {
        int(y)
        for y in years
        if _CFG.city_gdp_year_min <= int(y) <= _CFG.year_max
    }
    return sorted(ys, reverse=True)


def _parse_gdp_type(text: str) -> str:
    return "PPP" if "ppp" in (text or "").lower() else "Nominal"


def _parse_metric_type(text: str) -> str:
    low = (text or "").lower()
    if "%" in low or any(
        k in low for k in ("grew by", "growth", "year-on-year", "yoy", "compared to the previous year")
    ):
        return "growth"
    if "per capita" in low:
        return "per_capita"
    if "share of gdp" in low or "% of gdp" in low:
        return "share"
    return "level"


def _normalize_metric_type(raw: str | None) -> str:
    val = (raw or "").strip().lower()
    allowed = {"level", "growth", "share", "per_capita", "unknown"}
    if val in allowed:
        return val
    return "unknown"


def _parse_value_unit(text: str) -> str:
    low = (text or "").lower()
    if "trillion" in low or re.search(r"\btn\b", low):
        return "trillion"
    if "billion" in low or re.search(r"\bbn\b", low):
        return "billion"
    if "million" in low or re.search(r"\bmn\b", low):
        return "million"
    return "absolute"


def _parse_geo_level(text: str) -> str:
    low = (text or "").lower()
    if "metropolitan" in low or "metro" in low:
        return "Metro"
    return "City Proper"


def _normalize_currency(raw: str | None) -> str | None:
    return normalize_currency_code(raw)


def _normalize_value(raw: str, unit: str | None) -> float | None:
    try:
        val = float(raw.replace(",", ""))
    except ValueError:
        return None
    if not unit:
        return val
    unit_l = unit.lower()
    if unit_l in {"million", "m"}:
        return val * 1_000_000
    if unit_l in {"billion", "b"}:
        return val * 1_000_000_000
    if unit_l in {"trillion", "t"}:
        return val * 1_000_000_000_000
    return val


def _parse_scaled_number(text: str) -> tuple[float | None, str | None]:
    """Baseline-style value parsing that avoids selecting year tokens as GDP."""
    if not text:
        return None, None
    t = text.lower().replace("\u202f", " ").replace("\xa0", " ")
    unit = None
    scale = 1.0
    if "trillion" in t or re.search(r"\btn\b", t):
        unit, scale = "trillion", 1e12
    elif "billion" in t or re.search(r"\bbn\b", t):
        unit, scale = "billion", 1e9
    elif "million" in t or re.search(r"\bmn\b", t):
        unit, scale = "million", 1e6

    suffix = re.search(r"\b([-+]?\d[\d,]*\.?\d*)\s*([mbt])\b", t)
    if suffix:
        m = suffix.group(2)
        unit = {"m": "million", "b": "billion", "t": "trillion"}[m]
        scale = {"m": 1e6, "b": 1e9, "t": 1e12}[m]

    nums = re.findall(r"([-+]?\d[\d,]*\.?\d*)", t)
    if not nums:
        return None, unit

    # Use a broad year range here to avoid treating historical years
    # (e.g., 1991 in CEIC range strings) as GDP values.
    years = {int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", t) if 1900 <= int(y) <= 2100}
    values = []
    for raw in nums:
        c = raw.replace(",", "")
        try:
            v = float(c)
            if int(v) in years:
                continue
            values.append(v)
        except Exception:
            continue
    if not values:
        return None, unit
    return max(values) * scale, unit


def _parse_value_from_text(text: str) -> tuple[float | None, str | None, str | None]:
    best_value: float | None = None
    best_currency: str | None = None
    best_unit: str | None = None
    for p in _GDP_PATTERNS:
        for m in p.finditer(text or ""):
            raw_value = m.groupdict().get("value")
            raw_currency = m.groupdict().get("currency")
            raw_unit = m.groupdict().get("unit")
            if not raw_value:
                continue
            try:
                raw_float = float(raw_value.replace(",", ""))
            except Exception:
                continue
            # Avoid treating year tokens as GDP values when no unit is present.
            if raw_unit is None and 1900 <= int(raw_float) <= 2100:
                continue
            normalized = _normalize_value(raw_value, raw_unit)
            if normalized is None:
                continue
            if best_value is None or normalized > best_value:
                best_value = normalized
                best_currency = _normalize_currency(raw_currency)
                best_unit = raw_unit.lower() if raw_unit else None
    return best_value, best_currency, best_unit


def _parse_unit_hint(text: str) -> str | None:
    low = (text or "").lower()
    if "trillion" in low or re.search(r"\btn\b", low):
        return "trillion"
    if "billion" in low or re.search(r"\bbn\b", low):
        return "billion"
    if "million" in low or re.search(r"\bmn\b", low):
        return "million"
    return None


def _parse_plain_numeric_cell(cell_text: str) -> float | None:
    nums = re.findall(r"\b\d[\d,]*(?:\.\d+)?\b", cell_text or "")
    if not nums:
        return None
    # In table value cells we prefer the largest number token.
    values = []
    for n in nums:
        try:
            values.append(float(n.replace(",", "")))
        except Exception:
            continue
    return max(values) if values else None


def _apply_currency_guard(currency: str | None, text: str, country: str) -> str | None:
    cur = (currency or "").upper().strip() or None
    low = (text or "").lower()
    c = (country or "").strip().lower()
    if c == "china":
        has_cny_signal = any(k in low for k in (" yuan", "cny", "rmb", "renminbi", "人民币", " 元"))
        has_usd_signal = any(k in low for k in (" usd", "us$", " dollar", "$"))
        if has_cny_signal and (cur in {None, "JPY"}):
            return "CNY"
        # If both signals exist and model guessed USD, keep USD only when USD wording is explicit.
        if has_cny_signal and cur == "USD" and not has_usd_signal:
            return "CNY"
    return cur


def _is_year_confused_scaled_value(
    gdp_raw: float | None, year: int | None, evidence_text: str, content: str
) -> bool:
    if gdp_raw is None:
        return False
    text = f"{evidence_text or ''} {(content or '')[:4000]}".lower()
    checks: list[tuple[float, tuple[str, ...]]] = [
        (1e12, (" trillion", " tn", "tn ")),
        (1e9, (" billion", " bn", "bn ")),
        (1e6, (" million", " mn", "mn ")),
    ]
    for scale, tokens in checks:
        if not any(t in text for t in tokens):
            continue
        approx = gdp_raw / scale
        approx_int = int(round(approx))
        if 1900 <= approx_int <= 2100:
            if re.search(rf"\b{approx_int}\b\s*[-–]\s*(19\d{{2}}|20\d{{2}})\b", text):
                return True
            if re.search(rf"(19\d{{2}}|20\d{{2}})\s*[-–]\s*\b{approx_int}\b", text):
                return True
            if year is not None and abs(approx_int - int(year)) >= 10:
                return True
    return False


def _recover_value_from_evidence_first(evidence_text: str, content: str) -> float | None:
    ev = (evidence_text or "").strip()
    if ev:
        v, _ = _parse_scaled_number(ev)
        if v is not None:
            return v
        v2, _, _ = _parse_value_from_text(ev)
        if v2 is not None:
            return v2
    combined = f"{ev} {(content or '')[:4000]}"
    v3, _ = _parse_scaled_number(combined)
    if v3 is not None:
        return v3
    v4, _, _ = _parse_value_from_text(combined)
    return v4


def _maybe_scale_llm_numeric_from_unit_context(
    gdp_raw: float | None, evidence_text: str, content: str
) -> float | None:
    """
    If LLM returns an unscaled numeric (e.g., 207.058) while evidence says
    "EUR bn"/"billion", recover the scaled value from evidence context.
    """
    if gdp_raw is None:
        return None
    if abs(float(gdp_raw)) >= 1_000_000:
        return gdp_raw
    combined = f"{evidence_text or ''} {(content or '')[:4000]}".strip()
    if not combined:
        return gdp_raw
    unit_hint = _parse_unit_hint(combined)
    if unit_hint is None:
        return gdp_raw
    repaired = _recover_value_from_evidence_first(evidence_text, content)
    if repaired is None:
        return gdp_raw
    if repaired <= abs(float(gdp_raw)):
        return gdp_raw
    return repaired


def _looks_like_growth_rate_statement(evidence_text: str) -> bool:
    low = (evidence_text or "").lower()
    if not low:
        return False
    has_rate_signal = "%" in low or any(
        k in low
        for k in (
            "grew by",
            "growth",
            "increased by",
            "decreased by",
            "change",
            "compared to the previous year",
            "year-on-year",
            "yoy",
        )
    )
    has_level_signal = any(
        k in low
        for k in (
            " billion",
            " million",
            " trillion",
            " bn",
            " mn",
            " tn",
            " eur bn",
            " usd bn",
            " cny bn",
        )
    )
    return has_rate_signal and not has_level_signal


def _is_non_value_header(header: str) -> bool:
    low = (header or "").strip().lower()
    if not low:
        return False
    tokens = (
        "range",
        "frequency",
        "observations",
        "status",
        "source",
        "last update",
        "updated",
        "note",
    )
    return any(t in low for t in tokens)


def _is_value_like_header(header: str) -> bool:
    low = (header or "").strip().lower()
    if not low:
        return True
    return any(k in low for k in ("gdp", "last", "value", "amount", "level"))


def _parse_value_before_year_with_unit(text: str) -> tuple[float | None, str | None]:
    unit_hint = _parse_unit_hint(text)
    if not unit_hint:
        return None, None
    m = re.search(r"\b(\d[\d,]*(?:\.\d+)?)\s+(19\d{2}|20\d{2})\b", text or "")
    if not m:
        return None, None
    v = _normalize_value(m.group(1), unit_hint)
    return v, unit_hint


class ExtractorAgent:
    """Deterministic parser for GDP facts from HTML tables and text."""

    @staticmethod
    def _load_env() -> None:
        """Load root and tutorial-local .env files without overriding explicit env vars."""
        root_env = Path(__file__).resolve().parents[2] / ".env"
        local_env = Path(__file__).resolve().parents[1] / ".env"
        if root_env.exists():
            load_dotenv(root_env, override=False)
        if local_env.exists():
            load_dotenv(local_env, override=False)

    def _extract_from_tables(self, city: str, country: str, content: str) -> list[_Candidate]:
        if "<table" not in (content or "").lower():
            return []

        soup = BeautifulSoup(content, "lxml")
        tables = soup.find_all("table")
        out: list[_Candidate] = []
        city_l = city.lower()

        for ti, table in enumerate(tables):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            headers = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
            context_parts = []
            cap = table.find("caption")
            if cap:
                context_parts.append(cap.get_text(" ", strip=True))
            prev_header = table.find_previous(["h1", "h2", "h3", "h4"])
            if prev_header:
                context_parts.append(prev_header.get_text(" ", strip=True))
            context = " ".join(context_parts)
            context_has_gdp = "gdp" in context.lower() or "gross domestic product" in context.lower()
            context_scale = _parse_unit_hint(context)

            for ri, row in enumerate(rows[1:], start=1):
                cells = row.find_all(["th", "td"])
                if not cells:
                    continue
                row_text = " ".join(c.get_text(" ", strip=True) for c in cells)
                if city_l not in row_text.lower():
                    continue

                for ci, cell in enumerate(cells):
                    header = headers[ci] if ci < len(headers) else ""
                    if not context_has_gdp and "gdp" not in header.lower():
                        continue
                    if _is_non_value_header(header):
                        continue
                    if not _is_value_like_header(header):
                        continue
                    cell_text = cell.get_text(" ", strip=True)
                    combined = f"{header} | {row_text} | {cell_text} | {context}"
                    value, unit = _parse_value_before_year_with_unit(cell_text)
                    if value is None:
                        value, unit = _parse_scaled_number(cell_text)
                    currency = guess_currency(combined, country=country)
                    currency = _apply_currency_guard(currency, combined, country)
                    if value is not None and unit is None:
                        unit_hint = _parse_unit_hint(f"{header} {context} {row_text}") or context_scale
                        if unit_hint:
                            value = _normalize_value(str(value), unit_hint)
                            unit = unit_hint
                    if value is None:
                        plain = _parse_plain_numeric_cell(cell_text)
                        unit_hint = _parse_unit_hint(f"{header} {context} {row_text}") or context_scale
                        if plain is not None:
                            value = _normalize_value(str(plain), unit_hint)
                            unit = unit_hint
                    if value is None:
                        value2, unit2 = _parse_scaled_number(combined)
                        value, unit = value2, unit2
                    if value is None:
                        continue
                    years = _parse_years(combined)
                    year = years[0] if years else None
                    # Include row text so geography validation can verify the target city
                    # even when `_source_content` is truncated for very long pages.
                    evidence_text = f"{row_text} || {header} :: {cell_text}".strip()[:500]
                    fact = ExtractedGDPFact(
                        city=city,
                        country=country,
                        gdp_raw=value,
                        year=year,
                        currency=currency,
                        gdp_type=_parse_gdp_type(combined),
                        geo_level=_parse_geo_level(combined),
                        evidence_text=evidence_text,
                        evidence_path=f"table:{ti},{ri},{ci}",
                        metric_type=_normalize_metric_type(_parse_metric_type(combined)),
                        value_unit=unit or _parse_value_unit(combined),
                    )
                    conf = 0.65
                    if "gdp" in header.lower():
                        conf += 0.12
                    row_low = row_text.lower()
                    city_token = city.lower()
                    if f"gdp: {city_token}" in row_low or f"{city_token} gdp" in row_low:
                        conf += 0.10
                    if re.search(rf"\b(?:incl|including|excl|excluding)\s+{re.escape(city_token)}\b", row_low):
                        conf -= 0.30
                    if year:
                        conf += 0.08
                    if currency:
                        conf += 0.05
                    if unit:
                        conf += 0.05
                    out.append(_Candidate(fact=fact, confidence=min(conf, 0.95)))
        return out

    def _extract_from_text(self, city: str, country: str, content: str) -> list[_Candidate]:
        soup = BeautifulSoup(content or "", "lxml")
        text = soup.get_text("\n")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        out: list[_Candidate] = []
        city_l = city.lower()
        for i, ln in enumerate(lines):
            windows = [ln]
            if i + 1 < len(lines):
                windows.append(f"{ln} {lines[i + 1]}")
            if i + 2 < len(lines):
                windows.append(f"{ln} {lines[i + 1]} {lines[i + 2]}")

            for chunk in windows:
                low = chunk.lower()
                if city_l not in low:
                    continue
                if not ("gdp" in low or "gross domestic product" in low):
                    continue
                if "%" in low or "growth" in low or "per capita" in low:
                    continue
                gdp_raw, curr2, unit2 = _parse_value_from_text(chunk)
                unit = unit2
                if gdp_raw is None:
                    gdp_raw, unit = _parse_scaled_number(chunk)
                if gdp_raw is None:
                    continue
                curr = curr2 or guess_currency(chunk, country=country)
                curr = _apply_currency_guard(curr, chunk, country)
                # Guardrail for title-like year ranges (e.g., "GDP (2001-2024)").
                if (
                    re.search(r"\(\s*(19\d{2}|20\d{2})\s*-\s*(19\d{2}|20\d{2})\s*\)", chunk)
                    and unit is None
                    and curr is None
                ):
                    continue
                if gdp_raw < 1_000_000 and unit is None and curr is None:
                    continue
                years = _parse_years(chunk)
                year = years[0] if years else None
                if year is None:
                    continue
                fact = ExtractedGDPFact(
                    city=city,
                    country=country,
                    gdp_raw=gdp_raw,
                    year=year,
                    currency=curr,
                    gdp_type=_parse_gdp_type(chunk),
                    geo_level=_parse_geo_level(chunk),
                    evidence_text=chunk[:500],
                    evidence_path=f"line:{abs(hash(chunk)) % 1000000}",
                    metric_type=_normalize_metric_type(_parse_metric_type(chunk)),
                    value_unit=unit or _parse_value_unit(chunk),
                )
                conf = 0.55
                if curr:
                    conf += 0.05
                if unit:
                    conf += 0.05
                out.append(_Candidate(fact=fact, confidence=min(conf, 0.85)))
        return out

    def extract(self, city: str, country: str, content: str) -> list[ExtractedGDPFact]:
        candidates = self._extract_from_tables(city, country, content)
        candidates.extend(self._extract_from_text(city, country, content))
        if not candidates:
            return []

        # Prefer highest confidence, then newest year.
        candidates.sort(
            key=lambda c: (
                c.confidence,
                c.fact.year if c.fact.year is not None else -1,
                c.fact.gdp_raw if c.fact.gdp_raw is not None else -1.0,
            ),
            reverse=True,
        )
        return [candidates[0].fact]

    @staticmethod
    def _build_llm_context(content: str, city: str, max_chars: int = 12000) -> str:
        """
        Build an LLM-friendly text context from full HTML/text content.
        Avoid head-only truncation by prioritizing city+GDP mention windows.
        """
        if not content:
            return ""
        try:
            soup = BeautifulSoup(content, "lxml")
        except Exception:
            soup = BeautifulSoup(content, "html.parser")
        text = soup.get_text("\n")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            return (content or "")[:max_chars]

        city_l = (city or "").lower()
        gdp_keys = ("gdp", "gross domestic product", "grp", "gross value added", "gva")

        hit_idx: list[int] = []
        for i, ln in enumerate(lines):
            low = ln.lower()
            if city_l in low and any(k in low for k in gdp_keys):
                hit_idx.append(i)
        if not hit_idx:
            for i, ln in enumerate(lines):
                if city_l in ln.lower():
                    hit_idx.append(i)

        selected: list[str] = []
        seen = set()
        for i in hit_idx:
            for j in range(max(0, i - 2), min(len(lines), i + 3)):
                if j not in seen:
                    seen.add(j)
                    selected.append(lines[j])

        # If focused windows are sparse, include short head/tail context.
        if len(" ".join(selected)) < max_chars // 2:
            head = " ".join(lines[:80])
            tail = " ".join(lines[-80:])
            candidate = "\n".join([head, "\n".join(selected), tail]).strip()
        else:
            candidate = "\n".join(selected).strip()

        return candidate[:max_chars]

    def extract_with_llm(
        self, city: str, country: str, content: str, model: str = "openai:gpt-5-nano"
    ) -> list[ExtractedGDPFact]:
        """
        Bounded LLM fallback for ambiguous pages.
        Returns [] on any model/parser failure.
        """
        self.last_llm_error = None
        provider, model_name = self._resolve_provider_model(model)
        llm_context = self._build_llm_context(content or "", city=city, max_chars=12000)
        prompt = f"""
Extract one city-level GDP fact from the text below.
Return strict JSON only with keys:
gdp_raw, year, currency, gdp_type, geo_level, metric_type, evidence_text.
Use null if unknown.

City: {city}
Country: {country}
Text:
{llm_context}
""".strip()
        try:
            repair_actions: list[str] = []
            raw = retry_with_exponential_backoff(
                lambda: self._run_llm(provider, model_name, prompt),
                retries=3,
                retry_on_rate_limit_only=True,
            )
            try:
                data = self._parse_json_payload(raw)
            except Exception:
                data = self._coerce_raw_llm_output_to_payload(
                    raw=raw, city=city, country=country, content=content
                )
            if not isinstance(data, dict):
                self.last_llm_error = "llm_output_unparseable"
                return []
            gdp_raw = data.get("gdp_raw")
            year = data.get("year")
            try:
                gdp_raw = float(gdp_raw) if gdp_raw is not None else None
            except Exception:
                gdp_raw = None
            try:
                year = int(year) if year is not None else None
            except Exception:
                year = None
            evidence_text = str(data.get("evidence_text") or "")
            if _looks_like_growth_rate_statement(evidence_text):
                self.last_llm_error = "llm_payload_growth_rate_not_total"
                return []
            if gdp_raw is None:
                # Recover from common LLM outputs such as "207.058 EUR bn".
                fallback_text = " ".join(
                    [
                        str(data.get("gdp_raw") or ""),
                        evidence_text,
                        (content or "")[:4000],
                    ]
                )
                v, _, _ = _parse_value_from_text(fallback_text)
                if v is None:
                    v, _ = _parse_scaled_number(fallback_text)
                gdp_raw = v
                if gdp_raw is not None:
                    repair_actions.append("llm_value_recovered_from_fallback_text")
            if year is None:
                year_candidates = _parse_years(
                    " ".join([evidence_text, (content or "")[:3000]])
                )
                year = year_candidates[0] if year_candidates else None
                if year is not None:
                    repair_actions.append("llm_year_recovered_from_content")
            if _is_year_confused_scaled_value(gdp_raw, year, evidence_text, content):
                repaired = _recover_value_from_evidence_first(evidence_text, content)
                if repaired is not None:
                    gdp_raw = repaired
                    repair_actions.append("llm_year_confusion_repaired")
            gdp_before_scale = gdp_raw
            gdp_raw = _maybe_scale_llm_numeric_from_unit_context(gdp_raw, evidence_text, content)
            if (
                gdp_raw is not None
                and gdp_before_scale is not None
                and abs(float(gdp_raw) - float(gdp_before_scale)) > 1e-6
            ):
                repair_actions.append("llm_value_scaled_from_unit_context")
            guarded_currency = _apply_currency_guard(
                _normalize_currency(data.get("currency"))
                or guess_currency(
                    f"{evidence_text} {(content or '')[:2000]}",
                    country=country,
                ),
                f"{evidence_text} {(content or '')[:4000]}",
                country,
            )
            original_currency = _normalize_currency(data.get("currency"))
            if guarded_currency and original_currency and guarded_currency != original_currency:
                repair_actions.append("currency_guard_overrode_llm_currency")
            fact = ExtractedGDPFact(
                city=city,
                country=country,
                gdp_raw=gdp_raw,
                year=year,
                currency=guarded_currency,
                gdp_type=data.get("gdp_type") or _parse_gdp_type(content),
                geo_level=data.get("geo_level") or _parse_geo_level(content),
                evidence_text=evidence_text[:500],
                evidence_path="llm:json_fallback",
                metric_type=_normalize_metric_type(
                    data.get("metric_type") or _parse_metric_type(evidence_text)
                ),
                value_unit=_parse_value_unit(evidence_text),
                repair_actions=";".join(repair_actions) if repair_actions else "none",
            )
            if fact.gdp_raw is None:
                self.last_llm_error = "llm_payload_missing_numeric_value"
                return []
            return [fact]
        except Exception as e:
            self.last_llm_error = str(e).replace(";", ",")[:180]
            return []

    @staticmethod
    def _coerce_raw_llm_output_to_payload(
        *, raw: str, city: str, country: str, content: str
    ) -> dict:
        """
        Recover payload from non-JSON LLM outputs (common with prose replies).
        """
        text = (raw or "").strip()
        v, cur, _ = _parse_value_from_text(text)
        if v is None:
            v, _ = _parse_scaled_number(text)
        if v is None:
            combined = f"{text}\n{(content or '')[:4000]}"
            v, cur2, _ = _parse_value_from_text(combined)
            if v is None:
                v, _ = _parse_scaled_number(combined)
            if cur is None:
                cur = cur2

        years = _parse_years(text) or _parse_years((content or "")[:4000])
        year = years[0] if years else None
        currency = _normalize_currency(cur) or guess_currency(text, country=country)

        return {
            "gdp_raw": v,
            "year": year,
            "currency": currency,
            "gdp_type": _parse_gdp_type(text),
            "geo_level": _parse_geo_level(text),
            "metric_type": _normalize_metric_type(_parse_metric_type(text)),
            "evidence_text": (text or "")[:500],
        }

    @staticmethod
    def _resolve_provider_model(model: str) -> tuple[str, str]:
        model = (model or "").strip()
        if ":" in model:
            provider, name = model.split(":", 1)
            return provider.lower(), name
        if model.startswith("claude"):
            return "anthropic", model
        return "openai", model

    @staticmethod
    def _run_openai_http(model_name: str, prompt: str, api_key: str) -> str:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json={
                    "model": model_name,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,
                },
                timeout=60,
            )
            resp.raise_for_status()
            payload = resp.json()
            return (
                payload.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )
        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            # Some newer models reject chat/completions and require responses API.
            if status == 400:
                return ExtractorAgent._run_openai_responses_http(model_name, prompt, api_key)
            # Bubble up OpenAI body to make logs actionable.
            detail = ""
            try:
                detail = (e.response.text or "")[:300]
            except Exception:
                detail = ""
            if detail:
                raise RuntimeError(f"openai_http_error:{status}:{detail}")
            raise

    @staticmethod
    def _run_openai_responses_http(model_name: str, prompt: str, api_key: str) -> str:
        resp = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model_name,
                "input": prompt,
            },
            timeout=60,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            detail = ""
            try:
                detail = (e.response.text or "")[:300]
            except Exception:
                detail = ""
            status = getattr(getattr(e, "response", None), "status_code", None)
            if detail:
                raise RuntimeError(f"openai_responses_error:{status}:{detail}")
            raise
        payload = resp.json()
        # Prefer convenience field when present.
        if isinstance(payload.get("output_text"), str) and payload.get("output_text"):
            return payload["output_text"].strip()
        # Fallback: concatenate text chunks from output items.
        out_parts: list[str] = []
        for item in payload.get("output", []) or []:
            for part in item.get("content", []) or []:
                txt = part.get("text")
                if isinstance(txt, str) and txt:
                    out_parts.append(txt)
        return "\n".join(out_parts).strip()

    @staticmethod
    def _run_llm(provider: str, model_name: str, prompt: str) -> str:
        ExtractorAgent._load_env()
        if provider == "openai":
            from openai import OpenAI

            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise RuntimeError("llm_openai_api_key_missing")
            try:
                client = OpenAI(api_key=api_key)
                resp = client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                )
                return (resp.choices[0].message.content or "").strip()
            except TypeError as e:
                if "proxies" not in str(e):
                    raise
                return ExtractorAgent._run_openai_http(model_name, prompt, api_key)
        if provider == "anthropic":
            from anthropic import Anthropic

            api_key = os.getenv("ANTHROPIC_API_KEY")
            if not api_key:
                raise RuntimeError("llm_anthropic_api_key_missing")
            client = Anthropic(api_key=api_key)
            resp = client.messages.create(
                model=model_name,
                max_tokens=500,
                temperature=0.0,
                messages=[{"role": "user", "content": prompt}],
            )
            text_chunks = [getattr(x, "text", "") for x in resp.content]
            return "\n".join([t for t in text_chunks if t]).strip()
        raise ValueError(f"Unsupported provider: {provider}")

    @staticmethod
    def _parse_json_payload(raw: str):
        txt = (raw or "").strip()
        if txt.startswith("```"):
            txt = re.sub(r"^```(?:json)?\s*", "", txt)
            txt = re.sub(r"\s*```$", "", txt)
        return json.loads(txt)
