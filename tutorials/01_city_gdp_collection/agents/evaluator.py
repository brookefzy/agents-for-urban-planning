"""Evaluator agent: validation gates and run-level metrics."""

from __future__ import annotations

from collections import Counter

import pandas as pd
from utils.config import GDPConfig


class EvaluatorAgent:
    """Validation-first evaluator and run summary metrics."""

    def __init__(self, config: GDPConfig | None = None) -> None:
        self.config = config or GDPConfig()

    def evaluate_candidate(self, candidate: dict) -> tuple[bool, list[str]]:
        reasons: list[str] = []

        city = self._clean_scalar(candidate.get("city"))
        country = self._clean_scalar(candidate.get("country"))
        if not city or not country:
            reasons.append("geography_missing_city_or_country")
        else:
            method = str(self._clean_scalar(candidate.get("method")) or "")
            if method in {"direct_parser", "llm_fallback"}:
                source_content = str(self._clean_scalar(candidate.get("_source_content")) or "")
                # Enforce this check only when full source content is available.
                if source_content:
                    evidence_combined = (
                        str(self._clean_scalar(candidate.get("evidence_text")) or "")
                        + " "
                        + source_content
                    ).lower()
                    if city.lower() not in evidence_combined:
                        reasons.append("geography_city_not_in_evidence")
                geo_level = str(self._clean_scalar(candidate.get("geo_level")) or "").lower()
                if geo_level == "country":
                    reasons.append("geography_country_level_not_allowed")

        evidence_text = self._clean_scalar(candidate.get("evidence_text"))
        if not evidence_text:
            reasons.append("evidence_missing")
        metric_type = str(self._clean_scalar(candidate.get("metric_type")) or "unknown").lower()
        if metric_type in {"growth", "share", "per_capita"}:
            reasons.append("metric_type_not_level")
        if candidate.get("requires_evidence_path") and not self._clean_scalar(
            candidate.get("evidence_path")
        ):
            reasons.append("evidence_path_missing_for_table")

        year = self._clean_scalar(candidate.get("year"))
        try:
            year_i = int(year)
        except Exception:
            year_i = None
        if year_i is None:
            reasons.append("year_missing")
        elif not (self.config.city_gdp_year_min <= year_i <= self.config.year_max):
            reasons.append("year_out_of_bounds")

        gdp_usd = self._clean_scalar(candidate.get("gdp_usd"))
        if gdp_usd is None and self._clean_scalar(candidate.get("currency")) == "USD":
            gdp_usd = self._clean_scalar(candidate.get("gdp_raw"))
        try:
            gdp_usd_f = float(gdp_usd)
        except Exception:
            gdp_usd_f = None
        if gdp_usd_f is None:
            reasons.append("value_missing")
        else:
            if gdp_usd_f <= 0:
                reasons.append("value_non_positive")
            if gdp_usd_f < self.config.min_gdp_usd or gdp_usd_f > self.config.max_gdp_usd:
                reasons.append("value_out_of_bounds")

        pop = self._clean_scalar(candidate.get("population"))
        if pop not in (None, "", 0, "0") and gdp_usd_f is not None:
            try:
                pop_f = float(pop)
                if pop_f > 0:
                    per_capita = gdp_usd_f / pop_f
                    if per_capita < self.config.min_gdp_per_capita:
                        reasons.append("per_capita_too_low")
                    if per_capita > self.config.max_gdp_per_capita:
                        reasons.append("per_capita_too_high")
            except Exception:
                reasons.append("population_invalid")

        currency_raw = self._clean_scalar(candidate.get("currency"))
        currency = str(currency_raw).upper() if currency_raw is not None else ""
        if currency and currency != "USD":
            if candidate.get("usd_exchange_rate") in (None, ""):
                reasons.append("fx_missing_exchange_rate")
            if candidate.get("fx_year") in (None, ""):
                reasons.append("fx_missing_year")
            if candidate.get("gdp_usd") in (None, ""):
                reasons.append("fx_missing_usd_value")

        if not candidate.get("source_tier_label"):
            reasons.append("source_quality_missing")

        # Hallucination audit applies only to LLM fallback rows.
        method = str(self._clean_scalar(candidate.get("method")) or "")
        if method == "llm_fallback" and gdp_usd_f is not None:
            if not self._value_appears_in_source(candidate):
                reasons.append("hallucination_value_not_in_source")

        if candidate.get("method") == "downscaled_fallback" and candidate.get("status") != "inReview":
            reasons.append("fallback_status_must_be_inReview")

        return len(reasons) == 0, reasons

    @staticmethod
    def _clean_scalar(value):
        if value is None:
            return None
        if isinstance(value, str):
            v = value.strip()
            return v if v else None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        return value

    @staticmethod
    def _format_compact(x: float) -> str:
        if float(x).is_integer():
            return str(int(x))
        return f"{x:.2f}".rstrip("0").rstrip(".")

    def _value_appears_in_source(self, candidate: dict) -> bool:
        value = candidate.get("gdp_raw")
        try:
            v = float(value)
        except Exception:
            return True  # not an extraction hallucination case

        source_text = (
            str(candidate.get("_source_content") or "")
            + " "
            + str(candidate.get("evidence_text") or "")
        ).lower()
        source_text_nocomma = source_text.replace(",", "")

        variants = set()
        iv = int(round(v))
        variants.add(str(iv))
        variants.add(f"{iv:,}")
        variants.add(self._format_compact(v))
        variants.add(self._format_compact(v).replace(",", ""))
        if v >= 1_000_000:
            variants.add(f"{self._format_compact(v/1_000_000)} million")
        if v >= 1_000_000_000:
            variants.add(f"{self._format_compact(v/1_000_000_000)} billion")
        if v >= 1_000_000_000_000:
            variants.add(f"{self._format_compact(v/1_000_000_000_000)} trillion")

        for var in variants:
            low = var.lower()
            if low in source_text or low.replace(",", "") in source_text_nocomma:
                return True
        return False

    def summarize_run(self, candidates: list[dict], finals: list[dict]) -> dict:
        failures = Counter()
        passed = 0
        for row in candidates:
            ok, reasons = self.evaluate_candidate(row)
            if ok:
                passed += 1
            else:
                failures.update(reasons)
        return {
            "candidates_total": len(candidates),
            "candidates_passed": passed,
            "candidates_failed": len(candidates) - passed,
            "failure_reasons": dict(failures),
            "final_rows": len(finals),
            "weighted_quality_score_note": "provisional_source_tier_weights_v1",
        }
