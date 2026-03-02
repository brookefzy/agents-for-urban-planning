"""Runtime config and thresholds for GDP pipeline."""

from dataclasses import dataclass


@dataclass(frozen=True)
class GDPConfig:
    city_gdp_year_min: int = 2010
    year_max: int = 2025
    min_gdp_usd: float = 1.0
    max_gdp_usd: float = 10_000_000_000_000.0
    min_gdp_per_capita: float = 100.0
    # TODO(region-aware-thresholds): replace global per-capita bounds with
    # region/population-aware dynamic thresholds (e.g., metro class, income group).
    max_gdp_per_capita: float = 2_000_000_000_000.0
    top_k: int = 5
    required_search_env_any: tuple[str, str] = ("TAVILY_API_KEY", "SERPAPI_KEY")
