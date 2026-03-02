# Main GDP Workflow (WIP)

## Purpose
City-level GDP candidate collection with checkpointing, strict validation gates, and auditable outputs.

## Inputs
- `data/input/cityls.json`
- `data/input/city_meta.csv` (`urban_pop`)

## Outputs
- `data/output/gdp/r_city_gdp_candidates*.csv`
- `data/output/gdp/city_gdp_results*.csv`
- `data/output/gdp/run_evaluation*.json`

## SOP Steps
1. Load checkpoint from existing candidate CSV and skip processed `(city, country)` pairs.
2. Build multi-query set per city.
3. Retrieve candidate URLs via deterministic search tools.
4. Rank pre-fetch (dedupe, domain preference, snippet relevance).
5. Fetch/cache HTML/PDF for top candidates.
6. Rank post-fetch with year/geo/evidence signals.
7. Extract GDP facts (`gdp_raw`, `year`, `currency`, `gdp_type`, `geo_level`).
8. Normalize to USD with historical FX rates by year.
9. Apply fallback (`country_gdp_per_capita * metro_population`) only if direct extraction fails.
10. Run validation gates and mark failures with explicit reasons.
11. Write candidate-level, final-level, and evaluation outputs.

## Notes
- Keep `source_tier_label` and provisional numeric `source_tier`.
- Any score logic using numeric tier must be marked `TODO(source_tier_mapping)`.

## Runner
- Entrypoint: `workflows/run_gdp_pipeline.py`
- Current behavior: normalized input loading + checkpoint skip + dry-run exports.
- Planned expansion: retrieval, extraction, normalization, strict gate evaluation, and final ranking.
- Dry-run command:
  `PYTHONPATH="." python -c "from workflows.run_gdp_pipeline import run_pipeline; print(run_pipeline(dry_run=True))"`
