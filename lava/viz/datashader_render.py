"""Datashader rendering for scatter/heatmap above 100K rows."""

import base64
from io import BytesIO

import datashader as ds
import datashader.transfer_functions as tf
import plotly.graph_objects as go

from lava.engine.query import DataResult
from lava.llm.schema import VizSpec


def render_datashader(spec: VizSpec, data_result: DataResult) -> go.Figure:
    """Server-side rasterize, embed in Plotly for interactive axes."""
    cols = spec.columns or list(data_result.arrow_table.schema.names)
    x_col = cols[0]
    y_col = cols[1] if len(cols) > 1 else cols[0]

    pdf = data_result.arrow_table.select([x_col, y_col]).to_pandas()
    x_range = [float(pdf[x_col].min()), float(pdf[x_col].max())]
    y_range = [float(pdf[y_col].min()), float(pdf[y_col].max())]

    canvas = ds.Canvas(
        plot_width=800, plot_height=600,
        x_range=x_range, y_range=y_range,
    )

    if spec.chart_suggestion == "heatmap":
        agg = canvas.points(pdf, x_col, y_col, ds.count())
        img = tf.shade(agg, cmap="fire", how="log")
    else:
        agg = canvas.points(pdf, x_col, y_col)
        img = tf.shade(agg, cmap=["lightblue", "darkblue"], how="log")

    buf = BytesIO()
    img.to_pil().save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode()

    fig = go.Figure()
    fig.add_layout_image(
        source=f"data:image/png;base64,{b64}",
        x=x_range[0], y=y_range[1],
        sizex=x_range[1] - x_range[0],
        sizey=y_range[1] - y_range[0],
        xref="x", yref="y",
        sizing="stretch", layer="below",
    )
    fig.update_xaxes(range=x_range, title=x_col)
    fig.update_yaxes(range=y_range, title=y_col)
    fig.update_layout(
        title=spec.title,
        template="plotly_white",
        width=800, height=600,
    )
    return fig
