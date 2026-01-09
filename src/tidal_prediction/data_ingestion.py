"""Helpers for loading tidal datasets."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Sequence

ISO_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
)


@dataclass(frozen=True)
class TideSample:
    timestamp: datetime
    level: float


def _parse_timestamp(value: str) -> datetime:
    for fmt in ISO_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported timestamp format: {value}")


def load_csv(path: Path) -> List[TideSample]:
    """Load tidal samples from a CSV file with timestamp,level columns."""
    samples: List[TideSample] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row:
                continue
            try:
                timestamp_raw = row["timestamp"]
                level_raw = row["level"]
            except KeyError as exc:
                # Provide context about which row is malformed to aid debugging.
                raise ValueError(
                    f"Missing required column {exc.args[0]!r} in CSV row {reader.line_num}: {row}"
                ) from exc
            timestamp = _parse_timestamp(timestamp_raw.strip())
            level = float(level_raw.strip())
            samples.append(TideSample(timestamp=timestamp, level=level))
    return samples


def load_json(path: Path) -> List[TideSample]:
    """Load tidal samples from a JSON list of objects."""
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    samples: List[TideSample] = []
    for item in payload:
        timestamp = _parse_timestamp(str(item["timestamp"]))
        level = float(item["level"])
        samples.append(TideSample(timestamp=timestamp, level=level))
    return samples


def load_samples(path: Path) -> List[TideSample]:
    """Load tidal samples based on file extension."""
    if not path.exists():
        raise ValueError(f"load_samples: path does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"load_samples: path is not a file: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return load_csv(path)
    if suffix == ".json":
        return load_json(path)
    raise ValueError(f"load_samples: unsupported file extension {suffix!r} for path: {path}")


def split_timestamps_and_levels(
    samples: Sequence[TideSample],
) -> tuple[List[datetime], List[float]]:
    """Split tidal samples into parallel timestamp and level sequences."""
    times = [sample.timestamp for sample in samples]
    levels = [sample.level for sample in samples]
    return times, levels


def as_series(samples: Sequence[TideSample]) -> tuple[List[datetime], List[float]]:
    """
    Backwards-compatible wrapper for ``split_timestamps_and_levels``.

    Returns parallel lists of timestamps and levels for downstream modeling.
    """
    return split_timestamps_and_levels(samples)
def ensure_samples(data: Iterable[TideSample]) -> List[TideSample]:
    return list(data)
