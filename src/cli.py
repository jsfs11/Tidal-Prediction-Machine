"""Command-line interface for tidal prediction simulations."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from tidal_prediction.simulation import run_simulation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run tidal prediction simulations.")
    parser.add_argument("--input", required=True, help="Path to input CSV/JSON data.")
    parser.add_argument("--horizon", type=int, default=24, help="Prediction horizon in hours.")
    parser.add_argument("--output", help="Optional output CSV path.")
    parser.add_argument(
        "--start",
        help="ISO timestamp to start predictions (defaults to now).",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=60,
        help="Prediction step size in minutes.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.is_file():
        raise SystemExit(f"Input file does not exist: {input_path}")
    start_time = datetime.fromisoformat(args.start) if args.start else datetime.utcnow()
    result = run_simulation(
        input_path,
        start_time,
        horizon_hours=args.horizon,
        step_minutes=args.step,
    )

    output_lines = ["timestamp,prediction", *result.to_rows()]
    if args.output:
        output_path = Path(args.output)
        output_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
    else:
        print("\n".join(output_lines))


if __name__ == "__main__":
    main()
