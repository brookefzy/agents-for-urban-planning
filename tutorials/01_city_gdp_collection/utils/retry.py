"""Retry helpers with rate-limit awareness."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def is_rate_limit_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate limit" in text or "too many requests" in text


def retry_with_exponential_backoff(
    func: Callable[[], T],
    *,
    retries: int = 3,
    base_delay_seconds: float = 0.5,
    retry_on_rate_limit_only: bool = True,
) -> T:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as e:  # noqa: PERF203
            last_error = e
            if retry_on_rate_limit_only and not is_rate_limit_error(e):
                raise
            if attempt == retries:
                break
            time.sleep(base_delay_seconds * (2**attempt))
    raise RuntimeError(f"retry_exhausted: {last_error}")
