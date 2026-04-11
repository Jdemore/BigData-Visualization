"""LAVA — LLM-Assisted Visual Analytics. Entry point."""

import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv()

from lava.engine.bootstrap import bootstrap
from lava.engine.connection import get_connection
from lava.llm.client import warm_model
from lava.viz.app import app, init_app

ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")


def _ensure_api_key() -> None:
    """Prompt user for OpenAI API key if not set, and save to .env."""
    if os.environ.get("OPENAI_API_KEY"):
        return

    print("No OpenAI API key found.")
    print("Get one at: https://platform.openai.com/api-keys\n")
    key = input("Enter your OpenAI API key: ").strip()

    if not key:
        print("No key provided. The app will start but queries will fail.\n")
        return

    # Save to .env
    os.environ["OPENAI_API_KEY"] = key
    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.write(f"OPENAI_API_KEY={key}\n")
    print("Key saved to .env\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="LAVA — LLM-Assisted Visual Analytics")
    parser.add_argument(
        "--data",
        default=os.path.join("sample_data", "sales.csv"),
        help="Path to CSV or Parquet file to visualize",
    )
    parser.add_argument("--port", type=int, default=8050, help="Dash server port")
    args = parser.parse_args()

    if not os.path.exists(args.data):
        print(f"Error: data file not found: {args.data}")
        sys.exit(1)

    _ensure_api_key()

    # Bootstrap
    con = get_connection()
    table_name, column_stats = bootstrap(con, args.data)
    print(f"Loaded dataset '{table_name}' with {len(column_stats)} columns")

    # Warm the LLM
    if os.environ.get("OPENAI_API_KEY"):
        print("Warming GPT model...")
        warm_model()

    # Start Dash
    init_app(con, table_name, column_stats)
    print(f"Starting LAVA at http://127.0.0.1:{args.port}")
    app.run(debug=False, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
