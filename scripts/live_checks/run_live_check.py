"""Run a tracked live-check scenario and write a result bundle."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _ensure_src_on_path() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    src_path = repo_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    return repo_root


def main() -> None:
    """CLI entrypoint."""
    _ensure_src_on_path()

    from denbust.live_checks import run_live_check_scenario_sync

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path, help="Path to tracked scenario YAML")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Optional output root override (defaults to scenario output_root)",
    )
    args = parser.parse_args()

    report = run_live_check_scenario_sync(args.config, output_root=args.output_root)
    print(report.output_dir)


if __name__ == "__main__":
    main()
