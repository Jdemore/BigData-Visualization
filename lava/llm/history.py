"""Short-term conversation history for context-aware follow-up queries."""

from collections import deque
from dataclasses import dataclass

from lava.llm.schema import VizSpec

MAX_HISTORY = 3


@dataclass
class HistoryEntry:
    user_query: str
    refined_query: str
    spec: VizSpec


_history: deque[HistoryEntry] = deque(maxlen=MAX_HISTORY)


def add_entry(user_query: str, refined_query: str, spec: VizSpec) -> None:
    _history.append(HistoryEntry(user_query, refined_query, spec))


def get_history() -> list[HistoryEntry]:
    return list(_history)


def clear() -> None:
    _history.clear()


def build_history_block() -> str:
    """Format recent history as a compact prompt fragment. Empty string if none."""
    if not _history:
        return ""

    lines = ["Recent conversation (most recent last):"]
    for i, entry in enumerate(_history, 1):
        spec = entry.spec
        x = spec.x_axis.get("column", "?")
        y = spec.y_axis.get("column", "?") if spec.y_axis else "-"
        agg = (spec.y_axis.get("aggregation") if spec.y_axis else None) or "none"
        bucket = spec.x_axis.get("time_bucket") or "none"
        color = spec.color_by or "none"
        lines.append(
            f"  [{i}] \"{entry.user_query}\" → "
            f"{spec.chart_type}, x={x}, y={y}({agg}), "
            f"bucket={bucket}, color_by={color}"
        )
    lines.append(
        "If the new query is a follow-up (e.g. 'change it to a bar chart', "
        "'add region breakdown', 'make it weekly'), modify the most recent "
        "spec accordingly. Otherwise treat it as a fresh query."
    )
    return "\n".join(lines)
