---
name: evidence-table-validation
description: Use when returning a research table and each row must include evidence, uncertainty labels, and explicit failure reasons for missing or weak entries.
---

# Evidence Table Validation

## Purpose
Make table outputs reviewable by requiring row-level evidence and uncertainty labeling.

## Required Columns
Use these columns unless the user specifies a different schema:
- `city`
- `country`
- `year`
- `gdp_value_raw`
- `currency`
- `gdp_usd`
- `source_url`
- `source_organization`
- `source_tier`
- `evidence_text`
- `confidence`
- `status`
- `notes`

## Row Validation Rules
For each row:
1. `city` and `country` are present and match the requested place.
2. `year` is present (or row is marked `missing` with explanation).
3. `source_url` is present for non-missing rows.
4. `evidence_text` is present for non-missing rows.
5. `status` must be one of:
   - `ok`
   - `inReview`
   - `missing`
6. `notes` must explain uncertainty for `inReview` and `missing` rows.

## Confidence Guidance (Tutorial Version)
- `high`: official source, clear city-level value, evidence snippet directly supports value
- `medium`: reputable source but some ambiguity (year, geography, unit conversion)
- `low`: weak source or incomplete evidence; usually should also be `inReview`

## End-of-Task Checks
- Count rows by `status`
- List rows requiring manual review
- Summarize common reasons for uncertainty

## Important Rule
If evidence is missing, do not present the row as `ok`.
