# AGENTS INSTRUCTIONS (v2.2)

You are working inside the **WAT framework** (Workflow, Agents, Tools).  
Use LLMs for planning/ranking/edge-case reasoning, and deterministic code for data fetching, parsing, validation, normalization, and persistence.

## Task: GDP Collection Pipeline

Build a robust city-level GDP pipeline with strict validation gates and transparent audit outputs. This is a refactor of `_script/04_gdp_collection`

## Inputs and outputs

- **Input cities**: `data/input/cityls.json` (list of city objects).
- **Metro population**: `data/input/city_meta.csv`, field `urban_pop`.
- **Output folder**: `data/output/gdp/`.

Required outputs:
- `r_city_gdp_candidates.csv`: up to top-k candidates per city (default k=5).
- `city_gdp_results.csv`: exactly one final selected row per `(city,country)`.
- `run_evaluation.json`: component-level metrics and gating failures.

## Workflow definition

Workflows are markdown SOPs in `workflows/`. The GDP workflow should implement:

1. **Checkpointing**: Load existing `r_city_gdp_candidates.csv` to skip already processed `(city, country)` pairs.
2. Load `(city, country)` pairs from input.
3. Build multi-query search set per city (e.g., "[City] [Country] GDP 2024", "[City] [Country] GRP nominal").
4. Retrieve candidate URLs using deterministic search tools.
5. **Pre-fetch ranking**: Dedupe + domain preference (Source Tiering) + snippet relevance.
6. Fetch/cache content (HTML/PDF) for top candidates.
7. **Post-fetch ranking**: Incorporate extracted reporting year, geography level, and evidence quality.
8. **Extract candidate GDP facts**: Must identify `gdp_type` (Nominal/PPP) and `geo_level` (Metro/City Proper).
9. **Normalize currency**: Convert to USD using **historical FX rates** matching the data's `year`.
10. Apply **fallback** (country GDP-per-capita * metro population) only when direct city GDP extraction fails.
11. Run strict validation gates.
12. Export candidate-level and final-level outputs.

## Validation-first rules (must follow)

Every candidate row must pass these gates or be marked failed with explicit reasons:

1. **Geography gate**
   - Candidate city name and administrative parent (Country/State) must match input criteria.
2. **Evidence gate**
   - `evidence_text` exists and is traceable to source content.
   - For table extraction: include `evidence_path` (indices).
3. **Year gate**
   - Year is present and within `CITY_GDP_YEAR_MIN` to `YEAR_MAX`.
4. **Value gate**
   - GDP numeric value is positive and within `MIN_GDP_USD` to `MAX_GDP_USD`.
5. **Per-capita plausibility gate**
   - If population exists: check GDP-per-capita against `MIN_GDP_PER_CAPITA` / `MAX_GDP_PER_CAPITA`.
6. **Currency/FX gate**
   - If non-USD, `usd_exchange_rate` and `gdp_usd` must be populated using the correct `fx_year`.
7. **Source-quality gate**
   - Score based on Tier (1=Official, 2=Academic, 3=General).

Fallback handling rule:
- Any row produced via `method=downscaled_fallback` must be tagged for manual review with `status="inReview"`.
- `inReview` means human verification is required before final publication.

## LLM usage policy

LLM is **optional and bounded**.

Use LLM only for:
- Query expansion for low-recall cities.
- Ambiguous extraction fallback (parsing messy HTML/PDF).
- Identifying `geo_level` and `gdp_type` from context.

Do not use LLM for:
- Currency conversion math.
- Plausibility checks (deterministic bounds).
- API/Network calls.

## Retrieval & Source Tiering

Primary web sources: `SERPAPI_KEY` and `TAVILY_API_KEY`.

**Source Hierarchy:**
- **Tier 1 (High)**: Official Statistics (OECD, BEA, Eurostat, World Bank, National Stat Bureaus).
- **Tier 2 (Med)**: Academic publications, Reputable economic think-tanks (Brookings, Oxford Economics).
- **Tier 3 (Low)**: Wikipedia, News articles, General encyclopedias.

**Numeric mapping status (PENDING):**
- `source_tier` numeric encoding is a placeholder for now and will be finalized later.
- Until finalized, keep both fields in outputs:
  - `source_tier_label` (e.g., `tier1_official`, `tier2_academic`, `tier3_general`)
  - `source_tier` (numeric placeholder, nullable or temporary value)
- Mark all scoring logic that depends on numeric tier as `TODO(source_tier_mapping)`.

## Module responsibilities

### Layer 1: Workflows (`workflows/`)
- Define SOPs and input/output contracts.
- Manage state/checkpointing logic.

### Layer 2: Agents (`agents/`)

#### `search.py`
- Build country-specific query variants.
- Implement **Rate Limit Awareness**: use exponential backoff for API tools.

#### `extractor.py`
- Parse content and emit facts: `gdp_raw`, `year`, `currency`, `gdp_type`, `geo_level`.
- Identify the specific geography (e.g., "Metropolitan Area" vs "City Proper").

#### `normalizer.py`
- Map `currency` + `year` to historical FX rates.
- Apply downscaled fallback logic if direct extraction fails.

#### `evaluator.py`
- **Search Metrics**:
  - **Tier-1 Recall**: % of cities with at least one Tier-1 (Official) source in top-k results.
  - **Query Efficiency**: Track which query templates produce the final selected candidate.
  - **Source Diversity**: Ratio of unique domains vs. total URLs.
- **Extraction Metrics**:
  - **Information Extraction Rate**: % of URLs with successful extraction of (GDP, Year, Geo-level, Org).
  - **Hallucination Audit**: Deterministic check that `gdp_raw` exists within `evidence_text`.
  - **Schema Completeness**: % of extractions with all metadata fields populated.
- **Validation & Scientific Metrics**:
  - **Plausibility Delta (Z-Score)**: Deviation of extracted GDP-per-capita from National average (>3σ).
  - **Cross-Source Variance**: Coefficient of variation between multiple passing candidates.
  - **Agent-Human Agreement**: Accuracy against user-provided "Golden Labels".
- **Result Metrics**:
  - **Weighted Quality Score (PENDING)**: formula is provisional because `source_tier` numeric mapping is pending. Mark as `TODO(source_tier_mapping)` until finalized.

### Layer 3: Tools (`tools/`)
- Deterministic only: HTTP fetch, PDF parse, FX API, CSV I/O.
- Keep tools modular, testable, and side-effect aware.
- Keep credentials only in `_scripts/.env`.

## Output schemas

### `r_city_gdp_candidates.csv` (top-k per city)

Required columns:
- `city`, `country`, `population`
- `gdp_raw`, `year`, `currency`, `gdp_type`, `geo_level`
- `usd_exchange_rate`, `fx_year`, `gdp_usd`
- `source_url`, `source_tier`, `source_domain`, `source_organization`
- `method` (`direct_parser`, `llm_fallback`, `downscaled_fallback`)
- `evidence_text`, `evidence_path`
- `weighted_quality_score` (pending), `status`, `failure_reasons`
- `llm_used`, `model_name`

### `city_gdp_results.csv` (single best row per city)
- Selection order (updated): Most recent `year` > Highest `weighted_quality_score` > Highest source tier.

## Operating guidelines

1. Check existing tools before creating new ones.
2. When failures occur: read full trace, fix + retest, record failure mode.
3. If testing will consume paid credits, ask before running expensive calls.

## Directory layout
```
.tmp/           # Disposable intermediates and caches
tools/          # Deterministic execution (FX, Search, Fetch)
agents/         # Orchestration (SearchAgent, ExtractAgent)
workflows/      # SOP markdown docs (MainGDPWorkflow.md)
utils/          # Config, Logging, Checkpointing
```
## For sandbox testing
Use `source ~/.bash_profile && conda activate openai312` for loading required python module.
