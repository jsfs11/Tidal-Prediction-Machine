"""Simple harmonic tide model."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Sequence


@dataclass
class TideModel:
    """Basic harmonic tide predictor."""

    mean_level: float = 0.0
    amplitude: float = 1.0
    phase_offset: float = 0.0

    def fit(self, data: Sequence[float]) -> "TideModel":
        """Fit the model to observed tide levels."""
        if not data:
            raise ValueError("Training data must contain at least one value.")
        self.mean_level = sum(data) / len(data)
        min_level = min(data)
        max_level = max(data)
        self.amplitude = (max_level - min_level) / 2 if max_level != min_level else 1.0
        self.phase_offset = 0.0
        return self

    def predict(self, times: Iterable[datetime]) -> List[float]:
        """Predict tide levels for the given timestamps."""
        return [
            self.mean_level
            + self.amplitude
            * math.sin(
                2 * math.pi * _to_epoch_seconds(timestamp) / _M2_PERIOD_SECONDS
                + self.phase_offset
            )
            for timestamp in times
        ]


# Period (in seconds) of the principal lunar semi-diurnal (M2) tidal constituent.
# 12.42 hours is the commonly used approximate M2 period in harmonic tide models.
_M2_PERIOD_SECONDS = 12.42 * 3600


def _to_epoch_seconds(timestamp: datetime) -> float:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.timestamp()
