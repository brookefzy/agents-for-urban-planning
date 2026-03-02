"""UI trace helpers for mapping pipeline events to process rows and logs."""

from __future__ import annotations


def event_to_log_message(event: str, payload: dict) -> str:
    city = payload.get("city")
    country = payload.get("country")
    source_url = payload.get("source_url")

    if event == "city_start":
        return f"Start city: {city}, {country}"
    if event == "search_start":
        return f"Searching web for {city} ({payload.get('search_engine')})"
    if event == "search_empty":
        return f"No search results for {city}"
    if event == "search_error":
        return f"Search error for {city}: {payload.get('search_error')}"
    if event == "candidate_considered":
        return f"Candidate queued for {city}: {source_url}"
    if event == "candidate_evaluated":
        llm_tag = ""
        if payload.get("llm_used"):
            llm_tag = " | llm_used"
        elif payload.get("llm_attempted"):
            llm_tag = " | llm_attempted_no_fact"
        if payload.get("llm_error") and str(payload.get("llm_error")).lower() != "none":
            llm_tag += f" | llm_error={payload.get('llm_error')}"
        return (
            f"Evaluated candidate for {city}: status={payload.get('status')} "
            f"method={payload.get('method')}{llm_tag} "
            f"| failure_reasons={payload.get('failure_reasons') or 'none'} | url={source_url}"
        )
    if event == "candidate_failed":
        return (
            f"Candidate failed for {city}: {payload.get('failure_reasons')} "
            f"| url={source_url}"
        )
    if event == "fallback_used":
        return f"Used downscaled fallback for {city}"
    if event == "city_complete":
        return f"Completed city: {city} ({payload.get('rows_collected')} rows)"
    if event == "pipeline_complete":
        return (
            "Pipeline complete: "
            f"candidates={payload.get('candidate_rows_written')} "
            f"final={payload.get('final_rows_written')}"
        )
    return f"{event}: {payload}"


def event_to_process_row(timestamp: str, event: str, payload: dict) -> dict:
    status = "info"
    if event in {"candidate_failed", "search_error"}:
        status = "failed"
    elif event in {"candidate_evaluated", "pipeline_complete", "city_complete"}:
        status = "done"
    elif event in {"city_start", "search_start", "candidate_considered"}:
        status = "running"
    elif event == "fallback_used":
        status = "inReview"

    details_parts = []
    if payload.get("failure_reasons"):
        details_parts.append(str(payload.get("failure_reasons")))
    if payload.get("status"):
        details_parts.append(f"status={payload.get('status')}")
    if payload.get("method"):
        details_parts.append(f"method={payload.get('method')}")
    if payload.get("query"):
        details_parts.append(f"query={payload.get('query')}")
    if payload.get("llm_status"):
        details_parts.append(f"llm_status={payload.get('llm_status')}")
    if payload.get("llm_error") and str(payload.get("llm_error")).lower() != "none":
        details_parts.append(f"llm_error={payload.get('llm_error')}")

    return {
        "time": timestamp,
        "agent": payload.get("agent") or "Workflow",
        "stage": payload.get("stage") or event,
        "status": status,
        "city": payload.get("city") or "",
        "country": payload.get("country") or "",
        "candidate_url": payload.get("source_url") or "",
        "details": " | ".join(details_parts),
    }
