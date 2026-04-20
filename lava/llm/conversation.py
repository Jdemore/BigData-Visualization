"""Object-oriented multi-turn context helper. Mostly superseded by the
module-level history in history.py, but kept for tests and any future callers
that need more than one conversation at a time."""

from collections import deque

from lava.llm.schema import VizSpec


class ConversationContext:
    """Bounded deque of the last N (query, spec) pairs."""

    def __init__(self, max_turns: int = 3) -> None:
        self._history: deque[tuple[str, VizSpec]] = deque(maxlen=max_turns)

    def add(self, query: str, spec: VizSpec) -> None:
        self._history.append((query, spec))

    def build_history_prompt(self) -> str:
        if not self._history:
            return ""
        lines = ["Previous queries in this session:"]
        for q, s in self._history:
            lines.append(
                f'  - "{q}" -> chart: {s.chart_suggestion}, columns: {s.columns}'
            )
        return "\n".join(lines)
