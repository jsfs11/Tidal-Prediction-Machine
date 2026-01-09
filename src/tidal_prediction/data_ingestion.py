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
def _ensure_file_readable(path: Path) -> None:
    """Ensure that the given path points to an existing, regular file."""
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path}")


def load_csv(path: Path) -> List[TideSample]:
    """Load tidal samples from a CSV file with timestamp,level columns."""
    _ensure_file_readable(path)
    samples: List[TideSample] = []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not row:
                    continue
                timestamp = _parse_timestamp(row["timestamp"].strip())
                level = float(row["level"].strip())
                samples.append(TideSample(timestamp=timestamp, level=level))
    except OSError as exc:
        raise OSError(f"Failed to open input file: {path}") from exc
    return samples


def load_json(path: Path) -> List[TideSample]:
    """Load tidal samples from a JSON list of objects."""
    _ensure_file_readable(path)
    try:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except OSError as exc:
        raise OSError(f"Failed to open input file: {path}") from exc
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
