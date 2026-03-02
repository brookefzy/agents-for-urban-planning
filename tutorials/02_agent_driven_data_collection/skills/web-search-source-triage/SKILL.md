---
name: web-search-source-triage
description: Use when collecting numeric facts from the web and you need a consistent way to prioritize authoritative sources and flag weak sources.
---

# Web Search Source Triage

## Purpose
Improve retrieval quality by explicitly ranking sources before trusting extracted values.

## Source Tiers (Tutorial Version)
- `tier1_official`: government statistical agencies, central banks, OECD, World Bank, official city/regional statistical releases
- `tier2_reputable`: academic institutions, established think tanks, major institutional reports
- `tier3_general`: Wikipedia, news sites, blogs, summary pages, secondary aggregators

## Triage Rules
1. Prefer `tier1_official` sources when available.
2. Use `tier2_reputable` only when official city-level data is unavailable or unclear.
3. Treat `tier3_general` as lead-generation sources, not final evidence (unless clearly justified and flagged).
4. Record:
   - `source_url`
   - `source_organization`
   - `source_tier`
5. If multiple sources disagree, flag the row for manual review instead of picking one silently.

## Required Output Behavior
- Every row must include a source tier label.
- Rows using weak or ambiguous sources should be marked `status=inReview`.
- If only country-level data is available, report that as a failure to find city-level GDP (not as a successful row).

## Quick Decision Heuristic
- Official source + city mention + recent year + traceable evidence = strongest candidate
- General source + no evidence snippet + ambiguous geography = `inReview` or `missing`
