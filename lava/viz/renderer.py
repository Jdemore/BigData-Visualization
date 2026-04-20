"""VizSpec + query result -> Plotly figure.

The public entry point is render(). It dispatches on chart type and row count:
- Any chart, >100K rows scatter/heatmap -> Datashader (rasterized)
- Any chart, >5K rows -> server-side reduction via DuckDB
- Small results -> straight Plotly
- Charts that Plotly Express handles well use _PX_CHART_MAP; the rest (radar,
  waterfall, stacked/grouped bar, bubble, heatmap, table) need manual layout
  and go through _render_custom."""

import plotly.express as px
import plotly.graph_objects as go

from lava.engine.query import DataResult
from lava.llm.schema import VizSpec

# Plotly Express takes x/y/color kwargs and produces the right chart directly.
_PX_CHART_MAP = {
    "bar": px.bar,
    "line": px.line,
    "scatter": px.scatter,
    "histogram": px.histogram,
    "pie": px.pie,
    "box": px.box,
    "violin": px.violin,
    "area": px.area,
    "funnel": px.funnel,
    "treemap": px.treemap,
    "sunburst": px.sunburst,
    "bar_h": px.bar,
    "density_heatmap": px.density_heatmap,
    "strip": px.strip,
}

# Chart types that need hand-built go.Figure instances rather than a px.* call.
_CUSTOM_CHARTS = {"radar", "waterfall", "stacked_bar", "grouped_bar", "bubble", "table", "heatmap"}


def render(spec: VizSpec, data_result: DataResult, con=None) -> go.Figure:
    """Produce a Plotly figure for the given spec and query result.

    Picks a rendering strategy based on chart type and row count. Anything
    over 100K scatter points is rasterized by Datashader; anything over 5K is
    server-side reduced in DuckDB first. Below that we go straight to Plotly.
    """
    n = data_result.row_count
    chart = spec.chart_type

    if chart == "table":
        return _render_table(spec, data_result)

    if chart in ("scatter", "heatmap") and n > 100_000:
        from lava.viz.datashader_render import render_datashader
        return render_datashader(spec, data_result)

    if n > 5_000 and con is not None:
        from lava.viz.reduction import reduce_data
        pdf = reduce_data(spec, data_result, con)
    else:
        pdf = data_result.arrow_table.to_pandas()

    if chart in _CUSTOM_CHARTS:
        return _render_custom(spec, pdf)

    return _render_plotly(spec, pdf)


def _resolve_col(col: str, df_columns: list[str], spec: VizSpec) -> str:
    """Match a VizSpec column reference against the actual DataFrame columns.

    Falls through exact match -> label alias -> agg-suffixed alias -> prefix
    match. Needed because aggregation renames the SELECT output to the label
    or {col}_{agg}, so the column name in the spec may not appear verbatim.
    """
    if not isinstance(col, str):
        col = str(col)
    if col in df_columns:
        return col
    for axis in (spec.x_axis, spec.y_axis):
        if axis and axis.get("column") == col and axis.get("label"):
            if axis["label"] in df_columns:
                return axis["label"]
    agg = spec.aggregation or {}
    if col in agg:
        alias = f"{col}_{agg[col]}"
        if alias in df_columns:
            return alias
    for df_col in df_columns:
        if df_col.startswith(col + "_"):
            return df_col
    return col


def _pretty_label(col_name) -> str:
    """Turn snake_case_col or snake_case_col_sum into a nicely cased axis label."""
    if not isinstance(col_name, str):
        return str(col_name)
    parts = col_name.rsplit("_", 1)
    aggs = {"sum", "mean", "count", "min", "max", "median", "std"}
    if len(parts) == 2 and parts[1] in aggs:
        return f"{parts[0].replace('_', ' ').title()} ({parts[1].title()})"
    return col_name.replace("_", " ").title()


def _get_axes(spec: VizSpec, df_cols: list[str]) -> tuple[str, str | None, str | None]:
    """Return the DataFrame column names to use for x, y, and color."""
    x_col = _resolve_col(spec.x_axis["column"], df_cols, spec)

    y_col = None
    if spec.y_axis:
        y_agg = spec.y_axis.get("aggregation")
        y_label = spec.y_axis.get("label")
        # When the y-axis is aggregated and reuses the x column (e.g. y = COUNT(timestamp),
        # x = timestamp), naive lookup would resolve y back to the x column. The SQL output
        # column for an aggregated y is actually the label or {col}_{agg} -- check that first.
        if y_agg:
            alias = y_label or f"{spec.y_axis['column']}_{y_agg}"
            if alias in df_cols:
                y_col = alias
        if y_col is None:
            y_col = _resolve_col(spec.y_axis["column"], df_cols, spec)

    color_col = _resolve_col(spec.color_by, df_cols, spec) if spec.color_by else None
    if color_col and (color_col not in df_cols or color_col == x_col):
        color_col = None
    return x_col, y_col, color_col


def _build_labels(spec: VizSpec, x_col: str, y_col: str | None,
                  color_col: str | None) -> dict:
    """Human-readable axis-label overrides for Plotly's labels kwarg."""
    labels: dict = {}
    labels[x_col] = spec.x_axis.get("label") or _pretty_label(x_col)
    if y_col:
        labels[y_col] = (spec.y_axis.get("label") if spec.y_axis else None) or _pretty_label(y_col)
    if color_col:
        labels[color_col] = _pretty_label(color_col)
    return labels


def _render_plotly(spec: VizSpec, pdf) -> go.Figure:
    """Map VizSpec onto the right plotly.express call.

    Chart-type-specific kwargs live here because Plotly Express uses different
    parameter names (names/values for pie, path for treemap, etc.) and not a
    uniform x/y/color. Scatter at >1K points flips to WebGL for GPU rendering.
    """
    chart = spec.chart_type
    fn = _PX_CHART_MAP.get(chart, px.scatter)
    df_cols = list(pdf.columns)
    kwargs: dict = {}

    x_col, y_col, color_col = _get_axes(spec, df_cols)
    labels = _build_labels(spec, x_col, y_col, color_col)
    kwargs["labels"] = labels

    if chart == "pie":
        kwargs["names"] = x_col
        if y_col:
            kwargs["values"] = y_col
        kwargs["color_discrete_sequence"] = px.colors.qualitative.Set2
    elif chart == "histogram":
        kwargs["x"] = x_col
        if color_col:
            kwargs["color"] = color_col
    elif chart in ("treemap", "sunburst"):
        kwargs["path"] = [x_col]
        if color_col:
            kwargs["path"] = [color_col, x_col]
        if y_col:
            kwargs["values"] = y_col
    elif chart == "bar_h":
        kwargs["x"] = y_col or x_col
        kwargs["y"] = x_col if y_col else None
        kwargs["orientation"] = "h"
        if color_col:
            kwargs["color"] = color_col
        kwargs["color_discrete_sequence"] = px.colors.qualitative.Bold
    elif chart == "density_heatmap":
        kwargs["x"] = x_col
        if y_col:
            kwargs["y"] = y_col
        kwargs["color_continuous_scale"] = "Viridis"
    else:
        kwargs["x"] = x_col
        if y_col:
            kwargs["y"] = y_col
        if color_col:
            kwargs["color"] = color_col
        if chart in ("bar", "line", "scatter", "area", "funnel", "violin", "strip"):
            kwargs["color_discrete_sequence"] = px.colors.qualitative.Bold

    if chart == "scatter" and len(pdf) > 1000:
        kwargs["render_mode"] = "webgl"

    fig = fn(pdf, **kwargs)

    # Plotly leaves single-series traces unnamed, which hides the legend.
    # Force a name derived from the y/x column so the legend always shows.
    if not color_col and chart not in ("pie", "histogram", "treemap", "sunburst", "density_heatmap"):
        for trace in fig.data:
            if not trace.name or trace.name == "":
                trace.name = _pretty_label(y_col or x_col or "data")
                trace.showlegend = True

    _apply_layout(fig, spec)
    return fig


def _render_custom(spec: VizSpec, pdf) -> go.Figure:
    """Dispatch for chart types that Plotly Express doesn't cover directly."""
    chart = spec.chart_type
    df_cols = list(pdf.columns)
    x_col, y_col, color_col = _get_axes(spec, df_cols)

    if chart == "radar":
        return _render_radar(spec, pdf, x_col, y_col, color_col)
    elif chart == "waterfall":
        return _render_waterfall(spec, pdf, x_col, y_col)
    elif chart == "stacked_bar":
        return _render_bar_mode(spec, pdf, x_col, y_col, color_col, "stack")
    elif chart == "grouped_bar":
        return _render_bar_mode(spec, pdf, x_col, y_col, color_col, "group")
    elif chart == "bubble":
        return _render_bubble(spec, pdf, x_col, y_col, color_col)
    elif chart == "heatmap":
        return _render_heatmap(spec, pdf, x_col, y_col, color_col)
    else:
        # Unknown chart type shouldn't reach here (validated upstream), but if it
        # does, render a scatter so the user at least sees the data.
        fig = px.scatter(pdf, x=x_col, y=y_col)
        _apply_layout(fig, spec)
        return fig


def _render_radar(spec: VizSpec, pdf, x_col: str, y_col: str | None,
                  color_col: str | None) -> go.Figure:
    """Radar (spider) plot: categories as spokes, values as radial distance."""
    fig = go.Figure()
    categories = pdf[x_col].astype(str).tolist()

    if color_col and color_col in pdf.columns:
        for group_name, group_df in pdf.groupby(color_col):
            values = group_df[y_col].tolist() if y_col else group_df.iloc[:, 1].tolist()
            # Repeat first point so the polygon closes back on itself.
            values.append(values[0])
            cats = group_df[x_col].astype(str).tolist()
            cats.append(cats[0])
            fig.add_trace(go.Scatterpolar(
                r=values, theta=cats, fill="toself",
                name=str(group_name), opacity=0.7,
            ))
    else:
        values = pdf[y_col].tolist() if y_col else pdf.iloc[:, 1].tolist()
        values.append(values[0])
        categories.append(categories[0])
        fig.add_trace(go.Scatterpolar(
            r=values, theta=categories, fill="toself",
            name=_pretty_label(y_col or "value"),
        ))

    fig.update_layout(polar=dict(radialaxis=dict(visible=True)))
    _apply_layout(fig, spec)
    return fig


def _render_waterfall(spec: VizSpec, pdf, x_col: str, y_col: str | None) -> go.Figure:
    """Waterfall: sequential bars that add or subtract from a running total."""
    categories = pdf[x_col].astype(str).tolist()
    values = pdf[y_col].tolist() if y_col else pdf.iloc[:, 1].tolist()

    # First bar is the starting point (absolute); the rest are deltas from it.
    measure = ["relative"] * len(values)
    if len(measure) > 0:
        measure[0] = "absolute"

    fig = go.Figure(go.Waterfall(
        x=categories, y=values, measure=measure,
        connector=dict(line=dict(color="#888")),
        increasing=dict(marker=dict(color="#2ecc71")),
        decreasing=dict(marker=dict(color="#e74c3c")),
        totals=dict(marker=dict(color="#3498db")),
    ))
    _apply_layout(fig, spec)
    return fig


def _render_bar_mode(spec: VizSpec, pdf, x_col: str, y_col: str | None,
                     color_col: str | None, barmode: str) -> go.Figure:
    """Shared backend for stacked_bar and grouped_bar. barmode is 'stack' or 'group'."""
    fig = px.bar(pdf, x=x_col, y=y_col, color=color_col,
                 barmode=barmode,
                 labels=_build_labels(spec, x_col, y_col, color_col),
                 color_discrete_sequence=px.colors.qualitative.Bold)
    _apply_layout(fig, spec)
    return fig


def _render_bubble(spec: VizSpec, pdf, x_col: str, y_col: str | None,
                   color_col: str | None) -> go.Figure:
    """Scatter with a third numeric column mapped to marker size."""
    # Pick the first numeric column that isn't already on the x or y axis.
    numeric_cols = [c for c in pdf.columns if pdf[c].dtype.kind in "iufb"
                    and c != x_col and c != y_col]
    size_col = numeric_cols[0] if numeric_cols else None

    kwargs: dict = {"x": x_col, "labels": _build_labels(spec, x_col, y_col, color_col)}
    if y_col:
        kwargs["y"] = y_col
    if color_col:
        kwargs["color"] = color_col
    if size_col:
        kwargs["size"] = size_col
        kwargs["size_max"] = 40

    fig = px.scatter(pdf, **kwargs)
    if not size_col:
        fig.update_traces(marker=dict(size=12, opacity=0.7))
    _apply_layout(fig, spec)
    return fig


def _render_heatmap(spec: VizSpec, pdf, x_col: str, y_col: str | None,
                    color_col: str | None) -> go.Figure:
    """Pivot-style heatmap when a color_by is present; density heatmap otherwise."""
    if y_col and color_col:
        # Build the grid ourselves: color_by -> rows, x -> columns, y -> cell values.
        pivot = pdf.pivot_table(index=color_col, columns=x_col, values=y_col,
                                aggfunc="sum").fillna(0)
        fig = go.Figure(go.Heatmap(
            z=pivot.values, x=[str(c) for c in pivot.columns],
            y=[str(r) for r in pivot.index],
            colorscale="Viridis",
        ))
    elif y_col:
        fig = px.density_heatmap(pdf, x=x_col, y=y_col, color_continuous_scale="Viridis")
    else:
        fig = px.density_heatmap(pdf, x=x_col, color_continuous_scale="Viridis")
    _apply_layout(fig, spec)
    return fig


def _apply_layout(fig: go.Figure, spec: VizSpec) -> None:
    """Shared layout pass: title, template, legend, background, hover styling.
    Called from every renderer so the visual language stays consistent."""
    fig.update_layout(
        title=dict(
            text=spec.title,
            font=dict(size=20, color="#2c3e50"),
            x=0.5, xanchor="center",
        ),
        template="plotly_white",
        height=600,
        margin=dict(l=60, r=30, t=70, b=60),
        font=dict(family="Segoe UI, Helvetica, Arial, sans-serif", size=13),
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1, font=dict(size=12),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#ddd", borderwidth=1,
        ),
        plot_bgcolor="#fafafa",
        paper_bgcolor="white",
        hoverlabel=dict(bgcolor="white", font_size=13, bordercolor="#ccc"),
    )
    # These chart types don't have cartesian axes to style.
    if spec.chart_type not in ("radar", "pie", "treemap", "sunburst"):
        fig.update_xaxes(
            showgrid=True, gridwidth=1, gridcolor="#eee",
            title_font=dict(size=14, color="#555"), tickfont=dict(size=12),
        )
        fig.update_yaxes(
            showgrid=True, gridwidth=1, gridcolor="#eee",
            title_font=dict(size=14, color="#555"), tickfont=dict(size=12),
        )
    if spec.chart_type == "bar":
        fig.update_traces(marker_line_width=0)
        fig.update_layout(bargap=0.2)
    if spec.chart_type == "line":
        fig.update_traces(line=dict(width=2.5))
    if spec.chart_type in ("scatter", "bubble"):
        fig.update_traces(marker=dict(opacity=0.7, size=6))


def _render_table(spec: VizSpec, data_result: DataResult) -> go.Figure:
    """Plain tabular view, capped at 1000 rows. Used as the fallback for
    very exploratory queries where no other chart type fits."""
    pdf = data_result.arrow_table.slice(0, 1000).to_pandas()
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=[f"<b>{c}</b>" for c in pdf.columns],
            align="left", fill_color="#2c3e50",
            font=dict(color="white", size=13), height=35,
        ),
        cells=dict(
            values=[pdf[c] for c in pdf.columns],
            align="left",
            fill_color=[["#f8f9fa", "white"] * (len(pdf) // 2 + 1)],
            font=dict(size=12), height=28,
        ),
    )])
    fig.update_layout(
        title=dict(text=spec.title, font=dict(size=20, color="#2c3e50"),
                   x=0.5, xanchor="center"),
        height=600, margin=dict(l=20, r=20, t=60, b=20),
    )
    return fig
