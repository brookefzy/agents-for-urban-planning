"""Streamlit demo app for city GDP collection pipeline."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from ui_trace import event_to_log_message, event_to_process_row

from app_logic import (
    build_sample_csv_template,
    build_city_inputs_with_population,
    limit_cities_for_demo,
    load_uploaded_city_csv,
    write_pipeline_input_files,
)
from workflows.run_gdp_pipeline import run_pipeline

MAX_CITIES = 3
SEARCH_ENGINE = "tavily"
LLM_MODEL = "openai:gpt-5-nano"
URLS_PER_CITY_FOR_EXTRACTION = 5
MAX_URLS_TO_TRY_PER_CITY = 20


def _tutorial_root() -> Path:
    return Path(__file__).resolve().parent


def _load_env() -> None:
    load_dotenv(_tutorial_root() / ".env", override=False)


def _check_required_env() -> list[str]:
    _load_env()
    required = ["TAVILY_API_KEY", "OPENAI_API_KEY"]
    return [key for key in required if not os.getenv(key)]


def main() -> None:
    st.set_page_config(page_title="City GDP Collection Demo", layout="wide")
    st.title("City GDP Collection Demo")
    st.caption("Upload a CSV with `city,country` and run the GDP collection pipeline.")

    st.markdown("**CSV format:** required columns are `city,country`.")
    st.download_button(
        "Download Sample CSV Template",
        data=build_sample_csv_template(),
        file_name="city_template.csv",
        mime="text/csv",
    )
    uploaded = st.file_uploader("Upload city CSV", type=["csv"])

    if not uploaded:
        return

    try:
        cities_df = load_uploaded_city_csv(uploaded.getvalue())
    except ValueError as exc:
        st.error(str(exc))
        return

    if cities_df.empty:
        st.error("No valid `city,country` rows found in uploaded CSV.")
        return

    limited_df, truncated = limit_cities_for_demo(cities_df, max_cities=MAX_CITIES)
    if truncated:
        st.warning(
            f"Demo limit applied: file had {len(cities_df)} rows; processing only the first {MAX_CITIES}."
        )

    st.subheader("Input Preview")
    st.dataframe(limited_df, use_container_width=True)
    llm_research_agent_mode = st.checkbox(
        "LLM Research Agent Mode (use LLM directly on cached HTML when city name exists)",
        value=False,
    )
    allow_llm_fallback = st.checkbox(
        "Allow LLM fallback when deterministic parser has no fact",
        value=True,
    )
    llm_max_calls_total = st.number_input(
        "LLM max calls (total run)",
        min_value=1,
        max_value=200,
        value=20,
        step=1,
    )
    llm_max_calls_per_city = st.number_input(
        "LLM max calls (per city)",
        min_value=1,
        max_value=50,
        value=8,
        step=1,
    )
    parser_fallback_when_llm_research_fails = st.checkbox(
        "In research mode, fallback to deterministic parser if LLM returns no fact",
        value=True,
    )

    if not st.button("Run GDP Collection", type="primary"):
        return

    missing_env = _check_required_env()
    if missing_env:
        env_path = _tutorial_root() / ".env"
        st.error(
            "Missing API keys: "
            + ", ".join(missing_env)
            + "\n\n"
            + "Add them to: "
            + str(env_path)
            + "\nExample:\n"
            + "TAVILY_API_KEY=your_key\nOPENAI_API_KEY=your_key"
        )
        return

    logs: list[str] = []
    process_rows: list[dict] = []
    process_box = st.empty()
    live_box = st.empty()

    st.subheader("Process Trace")

    def log_line(text: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        logs.append(f"[{ts}] {text}")
        recent = logs[-14:]
        live_box.markdown("\n".join(f"- {line}" for line in recent))

    def push_process_row(row: dict) -> None:
        process_rows.append(row)
        process_box.dataframe(pd.DataFrame(process_rows), use_container_width=True, height=360)

    def push_manual_row(agent: str, stage: str, city: str, details: str, status: str = "running") -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        push_process_row(
            {
                "time": ts,
                "agent": agent,
                "stage": stage,
                "status": status,
                "city": city,
                "country": "",
                "candidate_url": "",
                "details": details,
            }
        )

    def progress_callback(event: str, payload: dict) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        push_process_row(event_to_process_row(ts, event, payload))
        log_line(event_to_log_message(event, payload))

    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = _tutorial_root() / ".tmp" / "webapp_runs" / run_stamp
    output_dir = run_dir / "output"

    with st.spinner("Running pipeline..."):
        log_line(
            "Run config: "
            f"llm_research_agent_mode={llm_research_agent_mode}, "
            f"allow_llm_fallback={allow_llm_fallback}, "
            f"llm_model={LLM_MODEL}, "
            f"urls_per_city_for_extraction={URLS_PER_CITY_FOR_EXTRACTION}, "
            f"max_urls_to_try_per_city={MAX_URLS_TO_TRY_PER_CITY}, "
            f"llm_max_calls_total={int(llm_max_calls_total)}, "
            f"llm_max_calls_per_city={int(llm_max_calls_per_city)}"
        )
        log_line("Starting population web search for uploaded cities.")
        push_manual_row(
            agent="PopulationLookup",
            stage="lookup_start",
            city="",
            details="Starting population web search for uploaded cities.",
            status="running",
        )
        inputs_with_pop = build_city_inputs_with_population(
            limited_df,
            search_engine=SEARCH_ENGINE,
            logger=log_line,
        )
        push_manual_row(
            agent="PopulationLookup",
            stage="lookup_complete",
            city="",
            details="Population lookup completed.",
            status="done",
        )

        cityls_path, city_meta_path = write_pipeline_input_files(inputs_with_pop, run_dir)
        log_line("Prepared temporary input files for pipeline execution.")

        artifacts = run_pipeline(
            cityls_path=cityls_path,
            city_meta_path=city_meta_path,
            output_dir=output_dir,
            dry_run=False,
            top_k=5,
            urls_per_city_for_extraction=URLS_PER_CITY_FOR_EXTRACTION,
            max_urls_to_try_per_city=MAX_URLS_TO_TRY_PER_CITY,
            search_engine=SEARCH_ENGINE,
            fail_on_missing_search_keys=True,
            allow_llm_fallback=allow_llm_fallback,
            llm_research_agent_mode=llm_research_agent_mode,
            parser_fallback_when_llm_research_fails=parser_fallback_when_llm_research_fails,
            llm_model=LLM_MODEL,
            llm_max_calls=int(llm_max_calls_total),
            llm_max_calls_per_city=int(llm_max_calls_per_city),
            use_checkpoint=False,
            resume=False,
            output_suffix="webapp",
            use_search_cache=True,
            progress_callback=progress_callback,
        )

    candidate_df = pd.read_csv(artifacts["candidate_csv"])
    final_df = pd.read_csv(artifacts["results_csv"])
    evaluation = json.loads(Path(artifacts["evaluation_json"]).read_text(encoding="utf-8"))

    st.subheader("Run Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Cities Processed", int(final_df[["city", "country"]].drop_duplicates().shape[0]))
    col2.metric("Candidate Rows", int(len(candidate_df)))
    col3.metric("LLM Fallback Rows", int(candidate_df["llm_used"].fillna(False).astype(bool).sum()))

    st.subheader("Failure Taxonomy")
    failure_series = candidate_df.get("failure_reasons")
    if failure_series is None:
        st.caption("No failure reasons found.")
    else:
        reason_counts: dict[str, int] = {}
        for raw in failure_series.fillna("").astype(str):
            for token in [t.strip() for t in raw.split(";") if t.strip()]:
                reason_counts[token] = reason_counts.get(token, 0) + 1
        if not reason_counts:
            st.caption("No failures in this run.")
        else:
            reason_df = pd.DataFrame(
                [{"failure_reason": k, "count": v} for k, v in reason_counts.items()]
            ).sort_values(by=["count", "failure_reason"], ascending=[False, True])
            st.dataframe(reason_df, use_container_width=True, height=180)

    st.subheader("Final Results")
    st.dataframe(final_df, use_container_width=True)

    st.subheader("Full Candidates (with Scores)")
    cand_view_cols = [
        c
        for c in [
            "city",
            "country",
            "year",
            "gdp_raw",
            "currency",
            "usd_exchange_rate",
            "gdp_usd",
            "source_tier_label",
            "weighted_quality_score",
            "prefetch_confidence",
            "prefetch_reasons",
            "quota_stage",
            "country_consistency",
            "metric_type",
            "value_unit",
            "repair_actions",
            "status",
            "method",
            "llm_status",
            "llm_error",
            "failure_reasons",
            "source_url",
        ]
        if c in candidate_df.columns
    ]
    cand_view = candidate_df[cand_view_cols].copy()
    if "weighted_quality_score" in cand_view.columns:
        cand_view["weighted_quality_score_num"] = pd.to_numeric(
            cand_view["weighted_quality_score"], errors="coerce"
        )
        cand_view = cand_view.sort_values(
            by=["city", "weighted_quality_score_num", "year"],
            ascending=[True, False, False],
            na_position="last",
        ).drop(columns=["weighted_quality_score_num"])
    st.dataframe(cand_view, use_container_width=True, height=360)

    st.subheader("Downloads")
    st.download_button(
        "Download city_gdp_results.csv",
        data=Path(artifacts["results_csv"]).read_bytes(),
        file_name="city_gdp_results.csv",
        mime="text/csv",
    )
    st.download_button(
        "Download r_city_gdp_candidates.csv",
        data=Path(artifacts["candidate_csv"]).read_bytes(),
        file_name="r_city_gdp_candidates.csv",
        mime="text/csv",
    )
    st.download_button(
        "Download run_evaluation.json",
        data=json.dumps(evaluation, indent=2),
        file_name="run_evaluation.json",
        mime="application/json",
    )

    with st.expander("View full process log", expanded=True):
        st.code("\n".join(logs) if logs else "No logs captured.")


if __name__ == "__main__":
    main()
