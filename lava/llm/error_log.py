"""Append-only JSONL logs for every pipeline run.

pipeline_runs.jsonl holds one record per query (success or failure) with the
raw LLM response, parsed VizSpec, and generated SQL. pipeline_errors.jsonl
holds a stack-trace per exception. Logs are NOT gitignored on purpose: keeping
them in the repo makes it easy to diff behaviour between runs and to feed bad
outputs back into prompt tuning."""

import json
import os
import traceback
from datetime import datetime, timezone

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
ERROR_LOG = os.path.join(LOG_DIR, "pipeline_errors.jsonl")
RUN_LOG = os.path.join(LOG_DIR, "pipeline_runs.jsonl")


def _ensure_dir() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)


def _safe_serialize(obj) -> str:
    """Serialize with default=str so datetimes, exceptions, etc. don't crash the logger."""
    try:
        return json.dumps(obj, default=str, ensure_ascii=False)
    except Exception:
        return str(obj)


def log_run(
    user_query: str,
    refined_query: str | None,
    refine_notes: str | None,
    llm_raw_response: dict | None,
    vizspec_dict: dict | None,
    sql: str | None,
    row_count: int | None,
    success: bool,
    error: str | None = None,
    duration_ms: float | None = None,
) -> None:
    """Append one pipeline-run record. Called for every query regardless of outcome."""
    _ensure_dir()
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_query": user_query,
        "refined_query": refined_query,
        "refine_notes": refine_notes,
        "llm_raw_response": llm_raw_response,
        "vizspec": vizspec_dict,
        "sql": sql,
        "row_count": row_count,
        "success": success,
        "error": error,
        "duration_ms": duration_ms,
    }
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        f.write(_safe_serialize(entry) + "\n")


def log_error(
    user_query: str,
    stage: str,
    error: Exception,
    context: dict | None = None,
) -> None:
    """Append one error record with stage, exception type, traceback, and caller context."""
    _ensure_dir()
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_query": user_query,
        "stage": stage,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": traceback.format_exc(),
        "context": context or {},
    }
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(_safe_serialize(entry) + "\n")


def get_recent_errors(n: int = 20) -> list[dict]:
    """Last N error entries as parsed dicts. Reads the whole file; fine for JSONL of this size."""
    if not os.path.exists(ERROR_LOG):
        return []
    entries: list[dict] = []
    with open(ERROR_LOG, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return entries[-n:]


def get_error_patterns() -> dict[str, int]:
    """Frequency table keyed on 'stage:error_type'. Useful for spotting recurring failures."""
    errors = get_recent_errors(100)
    patterns: dict[str, int] = {}
    for e in errors:
        key = f"{e.get('stage', '?')}:{e.get('error_type', '?')}"
        patterns[key] = patterns.get(key, 0) + 1
    return patterns
