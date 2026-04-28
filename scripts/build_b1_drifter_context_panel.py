"""Build the stored-output-only B1 drifter provenance panel assets."""

from __future__ import annotations

import json
from pathlib import Path

from src.services.panel_b1_drifter_context import run_panel_b1_drifter_context


def main() -> None:
    results = run_panel_b1_drifter_context(repo_root=Path(__file__).resolve().parents[1])
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
