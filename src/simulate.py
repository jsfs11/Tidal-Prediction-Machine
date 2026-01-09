"""Run a sample tidal prediction simulation."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from tidal_prediction.data_ingestion import load_samples
from tidal_prediction.simulation import latest_timestamp, run_simulation


def main() -> None:
    data_path = Path("data/sample_tides.csv")
    samples = load_samples(data_path)
    start_time = latest_timestamp(samples)
    result = run_simulation(data_path, start_time, horizon_hours=24, step_minutes=60)

    print("timestamp,prediction")
    for row in result.to_rows():
        print(row)


if __name__ == "__main__":
    main()
