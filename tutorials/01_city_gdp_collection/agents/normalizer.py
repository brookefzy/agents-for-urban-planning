"""Normalizer agent: currency conversion and fallback normalization."""

from __future__ import annotations

from tools.fx_rates import get_historical_fx_rate
from utils.currency import normalize_currency_code


class NormalizerAgent:
    """Normalizer for historical FX conversion and fallback GDP estimates."""

    def to_usd(
        self, value: float, currency: str, year: int
    ) -> tuple[float | None, float | None]:
        if value is None:
            return None, None
        cur = normalize_currency_code(currency)
        if not cur:
            return None, None
        if cur == "USD":
            return 1.0, float(value)
        fx = get_historical_fx_rate(cur, year)
        if fx is None:
            return None, None
        return fx, float(value) * float(fx)

    def downscaled_fallback(
        self, country_gdp_per_capita_usd: float, metro_population: float
    ) -> float:
        return float(country_gdp_per_capita_usd) * float(metro_population)

    def normalize_candidate(self, candidate: dict) -> dict:
        out = dict(candidate)
        gdp_raw = out.get("gdp_raw")
        currency = normalize_currency_code(out.get("currency"))
        if currency:
            out["currency"] = currency
        year = out.get("year")

        if gdp_raw is not None and currency and year:
            fx, usd = self.to_usd(float(gdp_raw), str(currency), int(year))
            out["usd_exchange_rate"] = fx
            out["fx_year"] = int(year)
            out["gdp_usd"] = usd
            out.setdefault("method", "direct_parser")
            out.setdefault("status", "ok")
            return out

        fallback_pc = out.get("country_gdp_per_capita_usd")
        population = out.get("population")
        if fallback_pc is not None and population is not None:
            out["gdp_usd"] = self.downscaled_fallback(float(fallback_pc), float(population))
            out["usd_exchange_rate"] = 1.0
            out["fx_year"] = int(year) if year else None
            out["method"] = "downscaled_fallback"
            out["status"] = "inReview"
            return out

        out.setdefault("status", "failed")
        return out
