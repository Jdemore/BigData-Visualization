"""Two-step NL to VizSpec pipeline: refine → generate, with full logging."""

import hashlib
import json
import time

from lava.llm.client import query_llm
from lava.llm.error_log import log_error, log_run
from lava.llm.parser import parse_llm_response
from lava.llm.prompt import (
    REFINE_SYSTEM,
    VIZSPEC_SYSTEM,
    build_context,
    build_refine_prompt,
    build_vizspec_prompt,
)
from lava.llm.schema import VizSpec

MAX_RETRIES = 1

_vizspec_cache: dict[str, VizSpec] = {}
_context_cache: str | None = None


def _cache_key(query: str) -> str:
    return hashlib.sha256(query.strip().lower().encode()).hexdigest()


def _get_context(column_stats: dict[str, dict]) -> str:
    global _context_cache
    if _context_cache is None:
        _context_cache = build_context(column_stats)
    return _context_cache


def _get_schema(column_stats: dict[str, dict]) -> dict[str, str]:
    return {col: info["type"] for col, info in column_stats.items()}


def _refine_query(user_query: str, context: str) -> tuple[str, str, str | None, dict | None]:
    """Step 1: Refine raw NL into a precise analytical query.
    Returns (refined_query, notes, chart_type_hint, raw_response)."""
    prompt = build_refine_prompt(user_query, context)
    try:
        data = query_llm(prompt, system=REFINE_SYSTEM)
        refined = data.get("refined_query", user_query)
        notes = data.get("notes", "")
        chart_hint = data.get("chart_type_hint")
        if not isinstance(chart_hint, str) or chart_hint == "null":
            chart_hint = None
        return refined, notes, chart_hint, data
    except Exception as e:
        log_error(user_query, "refine", e, {"prompt_length": len(prompt)})
        return user_query, "", None, None


def _generate_vizspec(
    user_query: str, refined_query: str, context: str, notes: str,
    schema: dict, chart_type_hint: str | None = None,
) -> tuple[VizSpec, dict | None]:
    """Step 2: Convert refined query into VizSpec.
    Returns (spec, raw_llm_response)."""
    prompt = build_vizspec_prompt(refined_query, context, notes, chart_type_hint)
    raw_response = None

    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            data = query_llm(prompt, system=VIZSPEC_SYSTEM)
            raw_response = data
            spec = parse_llm_response(data, schema)
            return spec, raw_response
        except (ValueError, json.JSONDecodeError, KeyError, TypeError) as e:
            last_error = e
            log_error(user_query, "generate_vizspec", e, {
                "attempt": attempt,
                "refined_query": refined_query,
                "raw_response": raw_response,
            })
            prompt = (
                f"{context}\n\n"
                f"Analytical query: {refined_query}\n\n"
                f"Your previous response was invalid: {e}\n"
                f"Please try again. Respond with valid JSON only."
            )

    first_col = list(schema.keys())[0]
    fallback = VizSpec(
        intent="explore",
        x_axis={"column": first_col, "aggregation": None, "time_bucket": None,
                "expression": None, "label": None},
        y_axis=None, color_by=None, filters=None,
        sort_by=None, limit=1000, chart_type="table",
        title=f"Exploring data (could not parse: {last_error})",
    )
    return fallback, raw_response


def nl_to_vizspec(
    user_query: str,
    column_stats: dict[str, dict],
    use_cache: bool = True,
) -> VizSpec:
    """Full two-step pipeline: NL → refine → VizSpec, with logging."""
    ck = _cache_key(user_query)
    if use_cache and ck in _vizspec_cache:
        return _vizspec_cache[ck]

    t0 = time.perf_counter()
    context = _get_context(column_stats)
    schema = _get_schema(column_stats)

    # Step 1: Refine
    refined_query, notes, chart_hint, refine_raw = _refine_query(user_query, context)

    # Step 2: Generate
    spec, vizspec_raw = _generate_vizspec(
        user_query, refined_query, context, notes, schema, chart_hint
    )

    duration_ms = (time.perf_counter() - t0) * 1000

    # Log the full run
    try:
        from lava.llm.sql_gen import vizspec_to_sql
        sql_preview = vizspec_to_sql(spec, "__table__")
    except Exception:
        sql_preview = None

    log_run(
        user_query=user_query,
        refined_query=refined_query,
        refine_notes=notes,
        llm_raw_response=vizspec_raw,
        vizspec_dict=spec.__dict__,
        sql=sql_preview,
        row_count=None,
        success=True,
        duration_ms=round(duration_ms, 1),
    )

    _vizspec_cache[ck] = spec
    return spec
