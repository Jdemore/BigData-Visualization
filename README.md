# LAVA — LLM-Assisted Visual Analytics

Ask natural language questions about your data, get interactive visualizations.

## Setup

**Requirements:** Python 3.13+, [UV](https://docs.astral.sh/uv/getting-started/installation/)

```bash
# Clone and install
git clone <repo-url>
cd BigData-Visualization
uv sync
```

## First Run

```bash
uv run python main.py
```

On first run you'll be prompted for your OpenAI API key:

```
No OpenAI API key found.
Get one at: https://platform.openai.com/api-keys

Enter your OpenAI API key: sk-...
Key saved to .env
```

The key is saved to `.env` and loaded automatically on future runs.

Then open **http://127.0.0.1:8050** and start asking questions.

## Usage

```bash
uv run python main.py                        # Default: sample sales data
uv run python main.py --data path/to/file.csv # Your own CSV or Parquet
uv run python main.py --port 3000             # Custom port
```

## Example Queries

- "Show total revenue by region"
- "Revenue trends over time, monthly"
- "What's the distribution of unit prices?"
- "Compare revenue per product category as a radar chart"
- "Top 10 products by revenue"

## Development

```bash
uv run pytest              # Run tests (71 tests)
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

## Architecture

```
User query → [LLM Refine] → [LLM VizSpec] → [SQL Gen] → [DuckDB] → [Plotly] → Browser
```

Three layers: Data Engine (DuckDB) | LLM Pipeline (GPT-4o-mini) | Viz Renderer (Plotly/Dash)

## Team

CS 43016 — Big Data, Kent State University
Joseph Demore, Randy Truong, Nathan Naples, Annika Hall, Cristian Quezada, Kregg Jackson
