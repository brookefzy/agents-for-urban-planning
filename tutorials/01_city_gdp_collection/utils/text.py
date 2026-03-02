"""Text helpers."""

import re


def clean_json_block(raw: str) -> str:
    """Strip optional fenced code markers around JSON/text blocks."""
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    return raw.strip()
