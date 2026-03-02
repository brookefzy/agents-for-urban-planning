"""Optional rendered HTML fetch helper.

This is an extension point for JS-heavy pages. The pipeline remains fully
functional without Playwright; when missing, callers should fallback safely.
"""

from __future__ import annotations


def fetch_rendered_text(url: str, timeout: int = 30) -> str | None:
    """
    Best-effort rendered fetch.
    TODO: wire full Playwright rendering path when browser runtime is available.
    """
    _ = (url, timeout)
    return None

