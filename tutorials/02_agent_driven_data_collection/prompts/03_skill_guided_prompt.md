# Prompt 3: Skill-Guided Agent Workflow (Main Tutorial Prompt)

Use this after loading the tutorial skills in `skills/`.

## Prompt
```text
Use your planning, source-triage, and evidence-validation skills to complete a research-grade city GDP extraction task.

Task:
Find city-level GDP values (not national GDP) for these cities:
- Zurich, Switzerland
- Nairobi, Kenya
- Ho Chi Minh City, Vietnam
- Auckland, New Zealand
- Oslo, Norway

Required workflow:
1. First produce a short plan and validation checklist.
2. Then execute the search/extraction workflow.
3. Apply source triage and evidence validation rules.
4. Mark uncertain rows as `inReview` with explicit reasons.

Required final outputs:
1. A final table with columns:
   city, country, year, gdp_value_raw, currency, gdp_usd, source_url, source_organization, source_tier, evidence_text, confidence, status, notes
2. A section titled `Rows Requiring Manual Review`
3. A short `Audit Note` summarizing:
   - strongest sources
   - weak sources
   - assumptions made
   - common failure modes encountered
   - what should be validated in a code-first workflow

Important constraints:
- Do not substitute country GDP for city GDP.
- Prefer official/statistical sources when available.
- If evidence is weak or missing, do not guess.
- Be explicit about uncertainty and missingness.
```

## Teaching Note
The point is not just a better answer. The point is a more defensible process with visible planning, triage, and validation.
