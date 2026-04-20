"""The VizSpec schema: the structured contract between the LLM and SQL generation.

Two models live here on purpose. VizSpecSchema (pydantic) is the JSON shape we
ask the LLM to produce. VizSpec (dataclass) is the internal runtime type that
SQL gen and the renderer work against. Keeping them separate lets us validate
LLM output aggressively without those checks leaking into the hot path."""

from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel


class AxisSpec(BaseModel):
    """One axis of a chart: which column, how to aggregate, how to bucket time."""

    column: str
    aggregation: Optional[str] = None  # sum, mean, count, min, max, median, std
    time_bucket: Optional[str] = None  # day, week, month, quarter, year
    expression: Optional[str] = None   # e.g. "revenue / quantity" for computed values
    label: Optional[str] = None        # display label override


class VizSpecSchema(BaseModel):
    """JSON shape requested from the LLM. Matches the schema documented in prompt.py."""

    intent: str
    x_axis: AxisSpec
    y_axis: Optional[AxisSpec] = None
    color_by: Optional[str] = None
    filters: Optional[list[dict]] = None
    sort_by: Optional[dict] = None
    limit: Optional[int] = None
    chart_type: str
    title: str


@dataclass
class VizSpec:
    """Post-validation spec consumed by sql_gen and the renderer. Always well-formed."""

    intent: str
    x_axis: dict
    y_axis: dict | None
    color_by: str | None
    filters: list[dict] | None
    sort_by: dict | None
    limit: int | None
    chart_type: str
    title: str

    @property
    def chart_suggestion(self) -> str:
        return self.chart_type

    @property
    def columns(self) -> list[str]:
        cols = [self.x_axis["column"]]
        if self.y_axis:
            cols.append(self.y_axis["column"])
        return cols

    @property
    def group_by(self) -> list[str] | None:
        return [self.color_by] if self.color_by else None

    @property
    def aggregation(self) -> dict[str, str] | None:
        agg: dict[str, str] = {}
        if self.x_axis.get("aggregation"):
            agg[self.x_axis["column"]] = self.x_axis["aggregation"]
        if self.y_axis and self.y_axis.get("aggregation"):
            col = self.y_axis.get("expression") or self.y_axis["column"]
            agg[col] = self.y_axis["aggregation"]
        return agg if agg else None


VALID_INTENTS = {"explore", "compare", "trend", "distribution", "correlation", "composition"}
VALID_CHARTS = {
    "bar", "line", "scatter", "histogram", "heatmap", "pie", "box", "table",
    "violin", "area", "funnel", "treemap", "sunburst", "radar", "waterfall",
    "stacked_bar", "grouped_bar", "bubble", "bar_h", "density_heatmap", "strip",
}
VALID_AGGS = {"mean", "sum", "count", "min", "max", "median", "std"}
VALID_OPS = {"=", "==", "!=", ">", "<", ">=", "<=", "contains", "not_contains"}
VALID_TIME_BUCKETS = {"day", "week", "month", "quarter", "year"}
