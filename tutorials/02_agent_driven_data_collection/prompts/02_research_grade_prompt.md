# Prompt 2: Research-Grade Prompt (No Skills Yet)

Use this after the baseline prompt to show how much improvement comes from stronger task framing alone.

## Prompt
```text
You are assisting with an urban planning research workflow. Find city-level GDP values (not national GDP) for the listed cities.

Cities:
- Zurich, Switzerland
- Nairobi, Kenya
- Ho Chi Minh City, Vietnam
- Auckland, New Zealand
- Oslo, Norway

Return a structured table with these columns:
- city
- country
- year
- gdp_value_raw
- currency
- gdp_usd (if conversion is possible)
- source_url
- source_organization
- evidence_text
- confidence (high / medium / low)
- status (ok / inReview / missing)
- notes

Rules:
- Prefer official statistics agencies, central banks, OECD, World Bank, or similar authoritative sources.
- Do not report country-level GDP as city GDP.
- If a value is ambiguous or not clearly city-level, mark `status=inReview` and explain why in `notes`.
- Include a short evidence snippet for every row.
- If no reliable city-level GDP is found, return `status=missing` and explain what was found instead.

After the table, include a short audit note (5-8 bullets) summarizing:
1. strongest sources used
2. weak sources used
3. assumptions
4. rows requiring manual verification
```

## What To Look For
- Better schema consistency
- Better uncertainty handling
- More explicit evidence and notes
- Still variable process quality (no guaranteed planning/validation workflow)
