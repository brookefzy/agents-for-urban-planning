# Prompt 1: Baseline (Intentionally Weak)

Use this first to show what happens when the agent receives a vague instruction.

## Prompt
```text
Find the 2024 GDP for these cities and return a table with city, country, GDP, and source.

Cities:
- Zurich, Switzerland
- Nairobi, Kenya
- Berlin, Germany
```

## What To Look For
- Missing or inconsistent schema (different columns/units)
- National GDP confused with city GDP
- Weak or missing evidence snippets
- Source quality not labeled
- No uncertainty handling

## Teaching Note
This prompt is useful because it often produces something that looks plausible but is hard to audit.
