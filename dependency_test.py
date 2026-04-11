"""Verify all LAVA dependencies are installed and importable."""

import importlib
import sys

DEPS = [
    ("duckdb", "duckdb"),
    ("pyarrow", "pyarrow"),
    ("psutil", "psutil"),
    ("numba", "numba"),
    ("google.genai", "google-genai"),
    ("pydantic", "pydantic"),
    ("plotly", "plotly"),
    ("dash", "dash"),
    ("dash_bootstrap_components", "dash-bootstrap-components"),
    ("datashader", "datashader"),
    ("colorcet", "colorcet"),
    ("PIL", "pillow"),
]


def main():
    ok, failed = [], []
    for module, package in DEPS:
        try:
            mod = importlib.import_module(module)
            version = getattr(mod, "__version__", "installed")
            ok.append((package, version))
        except ImportError as e:
            failed.append((package, str(e)))

    for package, version in ok:
        print(f"  [OK] {package} ({version})")

    for package, err in failed:
        print(f"  [FAIL] {package}: {err}")

    print(f"\n{len(ok)}/{len(DEPS)} dependencies OK")
    if failed:
        print(f"{len(failed)} failed — run `uv sync` to install")
        sys.exit(1)


if __name__ == "__main__":
    main()
