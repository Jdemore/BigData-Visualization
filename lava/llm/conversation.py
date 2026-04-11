"""Multi-turn conversation context for follow-up queries."""

from collections import deque

from lava.llm.schema import VizSpec


class ConversationContext:
    """Maintains last N query/spec pairs for follow-up resolution."""

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
