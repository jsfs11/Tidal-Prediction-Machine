"""Simulation orchestration for tidal predictions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Sequence

from tidal_prediction.data_ingestion import TideSample, as_series, load_samples
from tidal_prediction.models.tide_model import TideModel


@dataclass(frozen=True)
class SimulationResult:
    timestamps: List[datetime]
    predictions: List[float]

    def to_rows(self) -> List[str]:
        return [f"{timestamp.isoformat()},{prediction:.3f}" for timestamp, prediction in zip(self.timestamps, self.predictions)]


def run_simulation(
    input_path: Path,
    start_time: datetime,
    horizon_hours: int,
    step_minutes: int = 60,
) -> SimulationResult:
    samples = load_samples(input_path)
    timestamps, levels = as_series(samples)
    model = TideModel().fit(levels)
    prediction_times = _build_prediction_times(start_time, horizon_hours, step_minutes)
    predictions = model.predict(prediction_times)
    return SimulationResult(prediction_times, predictions)


def _build_prediction_times(
    start_time: datetime,
    horizon_hours: int,
    step_minutes: int,
) -> List[datetime]:
    total_steps = int(horizon_hours * 60 / step_minutes)
    return [start_time + timedelta(minutes=step_minutes * step) for step in range(total_steps + 1)]


def latest_timestamp(samples: Sequence[TideSample]) -> datetime:
    return max(sample.timestamp for sample in samples)
