"""Metrics for judging the LLM pipeline's output quality.

This is the evaluation harness referenced in section 7.5 of the report. It has
no committed labeled dataset yet; the functions are here ready to be driven
from a curated benchmark (see Future Work)."""

from dataclasses import dataclass

from lava.llm.schema import VizSpec


@dataclass
class EvalResult:
    """Aggregate metrics across a benchmark run."""

    parse_success_rate: float
    schema_compliance_rate: float
    intent_accuracy: float
    chart_score_avg: float
    avg_latency_ms: float


def calc_parse_success_rate(results: list[dict]) -> float:
    """Fraction of queries where the FIRST LLM attempt returned valid JSON.
    Retries count as failures here -- we want to measure cold accuracy."""
    if not results:
        return 0.0
    successes = sum(1 for r in results if r["success"] and r["attempts"] == 1)
    return successes / len(results)


def calc_intent_accuracy(predictions: list[str], labels: list[str]) -> float:
    """Classification accuracy for the intent field against human-labelled truth."""
    if not labels:
        return 0.0
    correct = sum(1 for pred, label in zip(predictions, labels) if pred == label)
    return correct / len(labels)


def score_chart_choice(spec: VizSpec, data_profile: dict) -> float:
    """Rule-based chart-fit score in [0, 1]. Does the chart type suit the data shape?
    Intentionally coarse -- tuned from common patterns, not learned."""
    chart = spec.chart_suggestion
    n_groups = data_profile.get("n_unique_groups", 1)
    has_time = data_profile.get("has_datetime_column", False)
    n_numeric = data_profile.get("n_numeric_columns", 0)

    score = 0.5
    if chart == "line" and has_time:
        score = 1.0
    elif chart == "bar" and 2 <= n_groups <= 20:
        score = 1.0
    elif chart == "scatter" and n_numeric >= 2:
        score = 1.0
    elif chart == "histogram" and n_numeric >= 1 and spec.intent == "distribution":
        score = 1.0
    elif chart == "pie" and n_groups <= 8:
        score = 0.9
    elif chart == "pie" and n_groups > 8:
        score = 0.2
    elif chart == "heatmap" and n_numeric >= 2 and n_groups > 5:
        score = 0.9
    elif chart == "table":
        score = 0.6
    return score
