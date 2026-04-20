"""System prompts and context builder for the two-stage NL pipeline.

The prompts themselves are long and specific -- they codify every lesson
learned from failed queries (boolean filters, distinct-day counting, the
percent-of-total pattern). Edits here change model behavior in production;
tread carefully and re-run the evaluation prompts after any change.
"""

# Step 1 prompt. Decides WHAT to compute -- which columns, what grouping,
# whether to bucket time, whether to add a computed ratio.
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
- CONDITION MATCHING: when the user describes an event or state (rained, delayed, shipped, in stock, failed, on sale), look for a boolean column OR a categorical column whose values express that condition, and state the implied filter explicitly. Example: "days it rained" on a schema with precipitation(boolean) and condition(Rain/Snow/Storm/...) -> filter precipitation = true (or condition IN ('Rain','Storm')).
- DISTINCT DAY COUNTING: when the user asks "how many days" and the time column has sub-day precision (timestamp with hours/minutes), state that the count must be of DISTINCT calendar days, not raw rows. Use the expression DISTINCT DATE_TRUNC('day', <timestamp_col>) with aggregation=count.
- Keep it concise -- one clear analytical statement.
"""

# Step 2 prompt. Decides HOW to execute -- the SQL structure, chart type,
# and axis mapping -- given step 1's refined analytical statement.
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
    "revenue / NULLIF(quantity, 0)" -- per-unit revenue
    "revenue * 100.0 / SUM(revenue) OVER ()" -- percentage of total
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

FILTERS:
- filters is a list of {"column", "op", "value"}. op is one of: =, !=, <, <=, >, >=, contains, not_contains. For booleans use op "=" with value true or false.
- When the user's question implies a condition ("rained", "delayed", "sold", "in stock", "failed"), ALWAYS add an entry to filters on the matching boolean/categorical column. Do NOT skip this. Example: dataset has precipitation(boolean) -> user says "rained" -> filters: [{"column": "precipitation", "op": "=", "value": true}]. If the dataset instead has condition(Rain/Snow/Storm/Clear/...) and no boolean, use op "contains" with value "Rain" or enumerate with multiple filter entries.
- NEVER use "contains" (ILIKE) on date/timestamp columns. For month-of-year filters use an expression filter: instead add the filter on an EXTRACT expression. Example: "december" -> filters: [{"column": "timestamp", "op": "=", "value": 12}] IS WRONG. Use a year-month range or do not use filters for time; instead rely on time_bucket+chart and an expression filter via x_axis.expression. If you must filter a month, prefer a date range: two filters, {"column": "timestamp", "op": ">=", "value": "2024-12-01"} and {"column": "timestamp", "op": "<", "value": "2025-01-01"}.

COUNTING DISTINCT DAYS:
- If the question asks "how many days" and the time column is a timestamp with sub-day precision, set y_axis.column = <timestamp_col>, y_axis.expression = "DISTINCT DATE_TRUNC('day', <timestamp_col>)", aggregation = "count". This counts distinct calendar days, not raw readings.
- Do the same for "how many weeks/months/years" with the matching DATE_TRUNC bucket.

PERCENT-OF-TOTAL PATTERN:
- When the user asks for "percent" or "share" of a total, do NOT put DISTINCT inside a window function -- DuckDB rejects COUNT(DISTINCT x) OVER (...).
- Correct pattern: compute the group aggregate normally, then divide by SUM(<that aggregate>) OVER (PARTITION BY <total-scope>).
- Example -- "percent of days by condition per station": y_axis.expression =
    "COUNT(DISTINCT DATE_TRUNC('day', timestamp)) * 100.0 / SUM(COUNT(DISTINCT DATE_TRUNC('day', timestamp))) OVER (PARTITION BY station_id)"
  with aggregation=null (because aggregates are already in the expression), x_axis=station_id, color_by=condition.

WORKED EXAMPLE -- "how many days did it rain in december, by station":
  filters: [
    {"column": "precipitation", "op": "=", "value": true},
    {"column": "timestamp", "op": ">=", "value": "2024-12-01"},
    {"column": "timestamp", "op": "<", "value": "2025-01-01"}
  ]
  x_axis: {"column": "station_id", "label": "Station"}
  y_axis: {"column": "timestamp", "expression": "DISTINCT DATE_TRUNC('day', timestamp)", "aggregation": "count", "label": "Rainy Days"}
  chart_type: "bar"
"""


def build_context(column_stats: dict[str, dict]) -> str:
    """Render column stats into the block of plain text the LLM sees at the top
    of each prompt. Cached per dataset by the pipeline; do not add per-request
    information here or you will break that cache."""
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
    """Row count isn't stored separately; it rides along on every column entry."""
    for info in column_stats.values():
        if "row_count" in info:
            return info["row_count"]
    return 0


def build_refine_prompt(user_query: str, context: str) -> str:
    """Assemble the user-message half of the step 1 call."""
    return f"{context}\n\nUser question: {user_query}\n\nRespond with JSON only."


def build_vizspec_prompt(
    refined_query: str, context: str, notes: str = "",
    chart_type_hint: str | None = None,
) -> str:
    """Assemble the user-message half of the step 2 call, carrying forward
    step 1's refined statement, any assumptions it made, and an optional
    chart override from the UI dropdown."""
    parts = [context, f"\nAnalytical query: {refined_query}"]
    if notes:
        parts.append(f"Analyst notes: {notes}")
    if chart_type_hint:
        parts.append(f"REQUIRED chart_type: {chart_type_hint}")
    parts.append("\nRespond with JSON only.")
    return "\n".join(parts)
