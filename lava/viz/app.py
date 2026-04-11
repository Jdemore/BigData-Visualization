"""Dash application — the web interface for LAVA."""

import time

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, callback, clientside_callback, dcc, html

from lava.engine.query import execute_query
from lava.llm.pipeline import nl_to_vizspec
from lava.llm.schema import VALID_CHARTS
from lava.llm.sql_gen import vizspec_to_sql
from lava.viz.renderer import render

# Module-level state set by init_app()
_con = None
_table_name = None
_column_stats = None

CHART_OPTIONS = [
    {"label": "Auto (LLM decides)", "value": "auto"},
    {"label": "Bar", "value": "bar"},
    {"label": "Bar (Horizontal)", "value": "bar_h"},
    {"label": "Stacked Bar", "value": "stacked_bar"},
    {"label": "Grouped Bar", "value": "grouped_bar"},
    {"label": "Line", "value": "line"},
    {"label": "Area", "value": "area"},
    {"label": "Scatter", "value": "scatter"},
    {"label": "Bubble", "value": "bubble"},
    {"label": "Histogram", "value": "histogram"},
    {"label": "Box Plot", "value": "box"},
    {"label": "Violin", "value": "violin"},
    {"label": "Strip", "value": "strip"},
    {"label": "Pie", "value": "pie"},
    {"label": "Treemap", "value": "treemap"},
    {"label": "Sunburst", "value": "sunburst"},
    {"label": "Radar", "value": "radar"},
    {"label": "Waterfall", "value": "waterfall"},
    {"label": "Funnel", "value": "funnel"},
    {"label": "Heatmap", "value": "heatmap"},
    {"label": "Density Heatmap", "value": "density_heatmap"},
    {"label": "Table", "value": "table"},
]

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

app.layout = dbc.Container(
    [
        # Header
        html.Div(
            html.H4("LAVA", className="text-white mb-0"),
            className="bg-primary px-3 py-2 rounded-top mt-3",
        ),
        # Input row: query + chart selector + button
        html.Div(
            [
                dbc.Input(
                    id="nl-input",
                    type="text",
                    placeholder="Ask a question about your data...",
                    debounce=False,
                    n_submit=0,
                    style={"fontSize": "16px", "flex": "1", "minWidth": "0"},
                    size="lg",
                ),
                dcc.Dropdown(
                    id="chart-type-select",
                    options=CHART_OPTIONS,
                    value="auto",
                    clearable=False,
                    style={"width": "180px", "flexShrink": "0"},
                ),
                dbc.Button(
                    "Visualize",
                    id="submit-btn",
                    n_clicks=0,
                    color="primary",
                    size="lg",
                    style={"flexShrink": "0"},
                ),
            ],
            className="mb-3",
            style={"display": "flex", "gap": "8px", "width": "100%",
                   "alignItems": "center"},
        ),
        # Status / loading
        dcc.Loading(
            html.Div(id="status-text", className="text-danger small mb-2"),
            type="circle",
        ),
        # Chart
        dcc.Graph(
            id="main-chart",
            style={"height": "70vh"},
            config={
                "displayModeBar": True,
                "displaylogo": False,
                "modeBarButtonsToRemove": ["lasso2d", "select2d"],
            },
        ),
        # Metadata footer
        html.Div(
            id="query-metadata",
            className="border-top mt-2 pt-2",
            style={"fontSize": "13px", "color": "#555", "fontFamily": "monospace"},
        ),
        dcc.Store(id="current-vizspec"),
    ],
    fluid=True,
    className="px-4",
)

# Escape key clears the input
clientside_callback(
    """
    function(id) {
        document.addEventListener('keydown', function(e) {
            var input = document.getElementById('nl-input');
            if (e.key === 'Escape' && document.activeElement === input) {
                input.value = '';
                input.dispatchEvent(new Event('input', {bubbles: true}));
            }
        });
        return window.dash_clientside.no_update;
    }
    """,
    Output("nl-input", "className"),
    Input("nl-input", "id"),
)


@callback(
    Output("main-chart", "figure"),
    Output("status-text", "children"),
    Output("query-metadata", "children"),
    Output("current-vizspec", "data"),
    Input("submit-btn", "n_clicks"),
    Input("nl-input", "n_submit"),
    State("nl-input", "value"),
    State("chart-type-select", "value"),
    prevent_initial_call=True,
)
def handle_query(n_clicks: int, n_submit: int, query_text: str, chart_override: str):
    if not query_text:
        return go.Figure(), "Enter a question above.", "", None

    if _con is None or _column_stats is None:
        return go.Figure(), "No dataset loaded.", "", None

    t0 = time.perf_counter()

    try:
        spec = nl_to_vizspec(query_text, _column_stats)

        # Apply chart type override from dropdown
        if chart_override and chart_override != "auto" and chart_override in VALID_CHARTS:
            spec.chart_type = chart_override

        sql = vizspec_to_sql(spec, _table_name)
        result = execute_query(_con, sql)
        fig = render(spec, result, _con)

        elapsed = time.perf_counter() - t0

        metadata = html.Div([
            html.Span(
                f"Rows: {result.row_count:,}  |  "
                f"Query: {result.query_ms:.0f}ms  |  "
                f"Total: {elapsed:.2f}s",
                style={"fontWeight": "bold"},
            ),
            html.Br(),
            html.Span("SQL: ", style={"fontWeight": "bold"}),
            html.Code(result.sql, style={"fontSize": "12px", "wordBreak": "break-all"}),
        ])

        warnings = " | ".join(result.warnings) if result.warnings else ""

        return fig, warnings, metadata, spec.__dict__

    except Exception as e:
        from lava.llm.error_log import log_error
        log_error(query_text, "render_callback", e, {
            "table_name": _table_name,
        })
        return go.Figure(), f"Error: {e}", "", None


def init_app(con, table_name: str, column_stats: dict[str, dict]) -> None:
    """Wire up the app with a dataset. Call before app.run()."""
    global _con, _table_name, _column_stats
    _con = con
    _table_name = table_name
    _column_stats = column_stats
