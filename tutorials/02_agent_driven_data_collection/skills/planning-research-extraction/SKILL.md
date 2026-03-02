---
name: planning-research-extraction
description: Use when starting a research data extraction task and you need a short plan, explicit schema, and validation checklist before execution.
---

# Planning Research Extraction

## Purpose
Create a lightweight plan before collecting data so the workflow is auditable and less likely to drift.

## Required Sequence
1. Restate the task and target unit of analysis (for example: city-level GDP, not country GDP).
2. Define the output schema before searching.
3. Define validation checks before extraction.
4. Define uncertainty handling (`ok`, `inReview`, `missing`).
5. Execute the task.
6. Summarize assumptions and unresolved issues.

## Output Template (Before Execution)
Use this structure:

```markdown
### Plan
- Objective:
- Unit of analysis:
- Target year(s):
- Output schema:

### Validation Checklist
- Geography check:
- Year check:
- Source quality check:
- Evidence check:
- Uncertainty rule:
```

## Rules
- Do not begin extraction before the schema and validation checklist are stated.
- Keep the plan short (5-10 bullets total).
- If the task is ambiguous, state the ambiguity and choose a conservative interpretation.
- Prefer explicit missingness over speculative filling.

## Common Failure Modes This Skill Prevents
- Mixing city and country indicators
- Changing columns mid-task
- Hiding assumptions until the end
- Reporting values without a validation standard
