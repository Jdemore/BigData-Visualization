"""Prompt architecture — two-step pipeline with rich column metadata."""

# Step 1: Refine the user's raw query into a precise analytical specification.
# This step decides WHAT to compute — bucketing, math, grouping strategy.
REFINE_SYSTEM = """You are a data analyst. Rewrite the user's question into a precise analytical query using ONLY the columns described.

Respond with ONLY a JSON object:
{
  "refined_query": "precise restatement with explicit column references, aggregation, and grouping",
  "chart_type_hint": "bar|bar_h|stacked_bar|grouped_bar|line|area|scatter|bubble|histogram|box|violin|strip|pie|treemap|sunburst|radar|waterfall|funnel|heatmap|density_heatmap|table|null",
  "notes": "brief assumptions made"
}

Rules:
- Reference exact column names from the schema.
- If the user explicitly requests a chart type (e.g. "bar chart", "scatter plot", "box plot"), set chart_type_hint to that type. If not specified, set null and let step 2 decide.
- If the query involves time, specify the interval (daily, weekly, monthly, quarterly, yearly) based on the date range span. For spans over 1 year, default monthly. For spans under 3 months, default daily. For spans 3-12 months, default weekly.
- If the query is vague like "show my data" or "best way to visualize", analyze the column metadata and suggest the most insightful view.
- Identify any computed values needed (ratios, percentages, per-unit, growth).
- Keep it concise — one clear analytical statement.
"""

# Step 2: Convert the refined analytical query into an executable VizSpec.
# This step decides HOW to execute — SQL structure, chart type, axis mapping.
VIZSPEC_SYSTEM = """You are a visualization engineer. Convert the analytical query into an executable visualization spec using ONLY the columns described.

Respond with ONLY a JSON object:
{
  "intent": "explore|compare|trend|distribution|correlation|composition",
  "x_axis": {
    "column": "column_name",
    "aggregation": null,
    "time_bucket": null,
    "expression": null,
    "label": "Display Label"
  },
  "y_axis": {
    "column": "column_name",
    "aggregation": "sum",
    "time_bucket": null,
    "expression": null,
    "label": "Display Label"
  },
  "color_by": null,
  "filters": [],
  "sort_by": null,
  "limit": null,
  "chart_type": "bar|bar_h|stacked_bar|grouped_bar|line|area|scatter|bubble|histogram|box|violin|strip|pie|treemap|sunburst|radar|waterfall|funnel|heatmap|density_heatmap|table",
  "title": "Chart Title"
}

AXIS FIELDS:
- column: REQUIRED. Must be an exact column name from the schema.
- aggregation: sum, mean, count, min, max, median, std, or null. When set, data is grouped by x_axis.column.
- time_bucket: day, week, month, quarter, year. ONLY on date/timestamp columns. Applies DATE_TRUNC to group by time intervals.
- expression: SQL expression for computed values. Use exact column names. Examples:
    "revenue / NULLIF(quantity, 0)" — per-unit revenue
    "revenue * 100.0 / SUM(revenue) OVER ()" — percentage of total
    Column names in expressions do NOT need quotes.
- label: human-readable display name for the axis.

CHART RULES:
- bar: x=categorical, y=numeric+agg. Comparison. <30 categories.
- bar_h: horizontal bar. Good when category names are long.
- stacked_bar: like bar but stacks color_by groups. Shows composition + comparison.
- grouped_bar: side-by-side bars per color_by group.
- line: x=date+time_bucket, y=numeric+agg. Trends over time.
- area: like line but filled. Good for cumulative/volume trends.
- scatter: x=numeric, y=numeric, NO aggregation. Correlation.
- bubble: like scatter but marker size encodes a third numeric column.
- histogram: x=numeric, no y_axis. Distribution.
- box: x=categorical, y=numeric, NO aggregation. Distribution comparison across groups.
- violin: like box but shows density shape. Better for larger datasets.
- strip: individual points by category. Good for small datasets.
- pie: x=categorical, y=numeric+agg. Composition. ONLY <8 categories.
- treemap: hierarchical composition. Use color_by for parent level.
- sunburst: like treemap but radial.
- radar: x=categorical (spokes), y=numeric. Compare profiles across groups. Good for <10 categories.
- waterfall: shows cumulative additions/subtractions. Good for financial breakdowns.
- funnel: sequential stages with decreasing values.
- heatmap: 2D grid, color=value. Needs x, y, and color_by.
- density_heatmap: 2D histogram showing point density.
- table: raw data display.
- color_by: ONLY low-cardinality categorical (<15 unique). NEVER dates or high-cardinality.

SORT/LIMIT:
- sort_by.column must match an output column name (use the label if aggregated).
- limit: only set when user asks for "top N" or "bottom N".
"""


def build_context(column_stats: dict[str, dict]) -> str:
    """Build rich column context from stats. Called once per dataset, cached."""
    lines = [f"Dataset: {_row_count(column_stats):,} rows\n", "Columns:"]

    for col, info in column_stats.items():
        kind = info["kind"]
        dtype = info["type"]
        uniq = info["unique_count"]
        line = f"  - {col} ({dtype}, {kind}, {uniq:,} unique values)"

        if kind == "numeric":
            line += f"  range: [{info.get('min')}, {info.get('max')}], mean: {info.get('mean')}"
        elif kind == "datetime":
            line += f"  range: [{info.get('min_date')} to {info.get('max_date')}], span: {info.get('span_days')} days"
        elif kind == "categorical" and info.get("top_values"):
            vals = info["top_values"][:8]
            line += f"  values: {', '.join(vals)}"
            if uniq > 8:
                line += f" ... ({uniq} total)"

        lines.append(line)

    return "\n".join(lines)


def _row_count(column_stats: dict[str, dict]) -> int:
    """Extract row count from any column's stats."""
    for info in column_stats.values():
        if "row_count" in info:
            return info["row_count"]
    return 0


def build_refine_prompt(user_query: str, context: str) -> str:
    """Build prompt for step 1: query refinement."""
    return f"{context}\n\nUser question: {user_query}\n\nRespond with JSON only."


def build_vizspec_prompt(
    refined_query: str, context: str, notes: str = "",
    chart_type_hint: str | None = None,
) -> str:
    """Build prompt for step 2: VizSpec generation."""
    parts = [context, f"\nAnalytical query: {refined_query}"]
    if notes:
        parts.append(f"Analyst notes: {notes}")
    if chart_type_hint:
        parts.append(f"REQUIRED chart_type: {chart_type_hint}")
    parts.append("\nRespond with JSON only.")
    return "\n".join(parts)
