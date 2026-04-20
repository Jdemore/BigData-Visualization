"""Dash front-end. Single-page layout with a file upload zone, query box,
chart-type override, a Plotly graph, and a SQL/timing footer.

Module-level state holds the active DuckDB connection, table name, and column
stats. This is intentionally simple: LAVA is single-user by design and all the
state fits naturally in a module. Moving to multi-user would require a
per-session cache (dcc.Store, Redis, or similar)."""

import base64
import os
import re
import time

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, callback, clientside_callback, dcc, html

from lava.engine.bootstrap import bootstrap
from lava.engine.query import execute_query
from lava.llm.pipeline import nl_to_vizspec
from lava.llm.schema import VALID_CHARTS
from lava.llm.sql_gen import vizspec_to_sql
from lava.viz.renderer import render

# Active-dataset handles. Replaced whenever init_app() runs or a user uploads.
_con = None
_table_name = None
_column_stats = None

UPLOAD_DIR = "uploads"
MAX_UPLOAD_MB = 200
ALLOWED_EXT = (".csv", ".json", ".ndjson", ".jsonl", ".parquet")

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
            [
                html.H4("LAVA", className="text-white mb-0 flex-grow-1"),
                html.Span(id="active-dataset", className="text-white-50 small"),
            ],
            className="bg-primary px-3 py-2 rounded-top mt-3 d-flex align-items-center",
        ),
        # Upload row
        html.Div(
            [
                dcc.Upload(
                    id="file-upload",
                    children=html.Div([
                        "Drag & drop or ",
                        html.A("select a file", className="text-primary"),
                        html.Span(
                            "  (CSV, JSON, Parquet -- up to 200 MB)",
                            className="text-muted small ms-2",
                        ),
                    ]),
                    style={
                        "width": "100%", "padding": "12px",
                        "borderWidth": "1px", "borderStyle": "dashed",
                        "borderRadius": "6px", "textAlign": "center",
                        "cursor": "pointer",
                    },
                    multiple=False,
                    max_size=MAX_UPLOAD_MB * 1024 * 1024,
                ),
                dcc.Loading(
                    html.Div(id="upload-status", className="small mt-1"),
                    type="dot",
                ),
            ],
            className="mt-3 mb-2",
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

# A tiny JS hook so Escape inside the query box clears it without a round trip.
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
    """End-to-end: NL -> VizSpec -> SQL -> Plotly figure. Runs on submit or Enter."""
    if not query_text:
        return go.Figure(), "Enter a question above.", "", None

    if _con is None or _column_stats is None:
        return go.Figure(), "No dataset loaded.", "", None

    t0 = time.perf_counter()

    try:
        spec = nl_to_vizspec(query_text, _column_stats)

        # Dropdown can force a specific chart type regardless of the LLM's choice.
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
    """Install the initial dataset. Main.py calls this once before app.run()."""
    global _con, _table_name, _column_stats
    _con = con
    _table_name = table_name
    _column_stats = column_stats
    app.layout.children[0].children[1].children = f"dataset: {table_name}"


def _safe_name(filename: str) -> str:
    """Strip filesystem-hostile characters from an uploaded filename so we can
    safely write it into UPLOAD_DIR without path-traversal risk."""
    base = os.path.basename(filename)
    return re.sub(r"[^A-Za-z0-9._-]", "_", base)


@callback(
    Output("upload-status", "children"),
    Output("active-dataset", "children"),
    Input("file-upload", "contents"),
    State("file-upload", "filename"),
    prevent_initial_call=True,
)
def handle_upload(contents: str, filename: str):
    """Decode the uploaded file, persist it to UPLOAD_DIR, and swap it in as
    the active dataset. The dcc.Upload component already enforces MAX_UPLOAD_MB
    client-side; we still validate the extension server-side before writing."""
    global _table_name, _column_stats

    if not contents or not filename:
        return "", f"dataset: {_table_name or 'none'}"

    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXT:
        return (
            html.Span(
                f"Unsupported file type: {ext}. Use CSV, JSON, or Parquet.",
                className="text-danger",
            ),
            f"dataset: {_table_name or 'none'}",
        )

    try:
        _, b64 = contents.split(",", 1)
        raw = base64.b64decode(b64)

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        dest = os.path.join(UPLOAD_DIR, _safe_name(filename))
        with open(dest, "wb") as f:
            f.write(raw)

        t0 = time.perf_counter()
        new_table, new_stats = bootstrap(_con, dest)
        elapsed = time.perf_counter() - t0

        _table_name = new_table
        _column_stats = new_stats

        size_mb = len(raw) / (1024 * 1024)
        msg = html.Span(
            f"Loaded '{new_table}' -- {len(new_stats)} columns, "
            f"{size_mb:.1f} MB, bootstrap {elapsed:.2f}s",
            className="text-success",
        )
        return msg, f"dataset: {new_table}"

    except Exception as e:
        from lava.llm.error_log import log_error
        log_error(filename, "upload_callback", e, {"filename": filename})
        return (
            html.Span(f"Upload failed: {e}", className="text-danger"),
            f"dataset: {_table_name or 'none'}",
        )
