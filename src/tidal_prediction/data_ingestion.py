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
        for row_num, row in enumerate(reader, start=2):  # start=2 accounts for header
            if not any(row.values()):  # Skip truly empty rows
                continue
            try:
                timestamp = _parse_timestamp(row["timestamp"].strip())
                level = float(row["level"].strip())
                samples.append(TideSample(timestamp=timestamp, level=level))
            except KeyError as e:
                raise ValueError(f"Missing required column in CSV row {row_num}: {e}")
            except ValueError as e:
                raise ValueError(f"Invalid data in CSV row {row_num}: {e}")
    return samples


def load_json(path: Path) -> List[TideSample]:
    """Load tidal samples from a JSON list of objects."""
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    samples: List[TideSample] = []
    for idx, item in enumerate(payload):
        try:
            timestamp = _parse_timestamp(str(item["timestamp"]))
            level = float(item["level"])
            samples.append(TideSample(timestamp=timestamp, level=level))
        except KeyError as e:
            raise ValueError(f"Missing required field in JSON item {idx}: {e}")
        except ValueError as e:
            raise ValueError(f"Invalid data in JSON item {idx}: {e}")
    return samples


def load_samples(path: Path) -> List[TideSample]:
    """Load tidal samples based on file extension."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return load_csv(path)
    if suffix == ".json":
        return load_json(path)
    raise ValueError(f"Unsupported file extension: {suffix}")


def as_series(samples: Sequence[TideSample]) -> tuple[List[datetime], List[float]]:
    """Return timestamps and levels for downstream modeling."""
    times = [sample.timestamp for sample in samples]
    levels = [sample.level for sample in samples]
    return times, levels
