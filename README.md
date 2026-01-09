# Tidal-Prediction-Machine

## Overview
This repository provides a minimal tidal prediction package with data ingestion, a basic harmonic model, and orchestration tools for running simulations.

## Package layout
- `src/tidal_prediction/data_ingestion.py`: Load CSV/JSON tidal observations.
- `src/tidal_prediction/models/tide_model.py`: `TideModel` with `fit` and `predict`.
- `src/tidal_prediction/simulation.py`: Simulation runner and utilities.
- `src/cli.py`: CLI entry point.
- `src/simulate.py`: Sample simulation script.
- `data/sample_tides.csv`: Small sample dataset.

## Usage
Run the sample simulation (prints to stdout):

```bash
python src/simulate.py
```

Run via CLI with explicit parameters:

```bash
python src/cli.py --input data/sample_tides.csv --horizon 24 --step 60 --output results.csv
```

Input data expects `timestamp` and `level` columns in CSV (or equivalent JSON objects). The output is a CSV with `timestamp,prediction` rows.
