# DSAI 3202 Project Phase 1: Flight arrival delay data pipeline

Group 3 | Student: TODO | Submitted commit: TODO (fill in before submitting)

## Project and hypothesis

- Task: predict whether a scheduled US flight arrives 15 or more minutes late (`ArrDel15 = 1`), two hours before departure.
- Baseline: logistic regression on carrier, origin, destination, scheduled hour, day of week and distance.
- Hypothesis: a gradient-boosted tree model with engineered features (origin traffic, recent delay rates by airport, carrier and route) beats the baseline by at least 0.05 PR-AUC on unseen later flights. Under 0.02 refutes it.
- Split by date: train Jan-Apr, validate May, test Jun, deploy Jul 2022.
- Approved refinements: TODO (none so far)

## Dataset and architecture

- Source: Kaggle, `robikscube/flight-delay-dataset20182022`, file `Combined_Flights_2022.csv`. Kaggle version: TODO
- Size: 1,415,293,445 bytes, 61 columns, 4,078,318 data rows (Jan-Jul 2022).
- Ingestion: batch, manual, one static snapshot. A new version would go to `v2/` with its own manifest.
- Source receipt: `evidence/source-manifest.json` (SHA-256 `b4cde15b526c365f81a08446a3c53bf83682b2bcf95e1d33d20368a9807824a3`).
- Lake layout (one ADLS Gen2 account):
  - `raw/flights/combined_flights_2022/v1/` : original CSV and `_manifest.json`
  - `processed/flights/combined_flights_2022/v1/Combined_Flights_2022.parquet` : single Parquet copy (Lab 3)
  - `processed/flights/combined_flights_2022/v1/bronze/month=01..07/` : Parquet by month with `data_split` (Lab 4)
  - `curated/flights/combined_flights_2022/v1/landing-summary.json`
- Architecture diagram: TODO (laptop -> VM -> ADLS raw -> Azure Data Factory data flow on Azure IR -> ADLS processed)
- Schema: `evidence/schema.tsv`

## Run the pipeline

Prerequisites: Azure VM with Python 3, Azure CLI, AzCopy; an ADLS Gen2 lake; an Azure Data Factory with a managed identity that has Storage Blob Data Contributor on the lake.

1. `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
2. `source ~/.config/dsai3202/lab3.env; export SA RG LOC`
3. `python3 tools/flights.py manifest <laptop-sha256>`
4. `python3 tools/flights.py inspect`
5. Upload with AzCopy (`--overwrite=false`) to `raw/.../v1/`, then `python3 tools/flights.py verify`
6. `python3 tools/flights.py convert` and publish to `processed`, then `publish-check`
7. Run `pl_transform_flights` once in ADF Studio, then download `bronze/` and run `python3 tools/check_bronze.py`

ADF definitions are exported in `adf/`. Rerunning `pl_transform_flights` requires deleting the `bronze/` folder first (see Results).

## Results and features

- Raw copy verified: source and raw hashes match (`evidence/verify-1.txt`).
- Retry check: TODO (`evidence/receipts-before.tsv` vs `receipts-after.tsv`)
- Parquet conversion row accounting: TODO (`evidence/convert.txt`)
- ADF run ID: TODO. rowsRead / rowsWritten: TODO
- Failed check kept as evidence: the first run wrote the data 3 times (12,234,954 rows against 4,078,318 expected); `evidence/bronze-check-FAILED-3x-duplicate.txt`. Cause: TODO. Fixed by deleting `bronze/` and running once.
- Final check: `evidence/bronze-check.txt` TODO

| Month | Rows |
|---|---|
| 1 | 563,737 |
| 2 | 519,952 |
| 3 | 590,542 |
| 4 | 580,290 |
| 5 | 602,950 |
| 6 | 602,057 |
| 7 | 618,790 |

- Quality, EDA, catalog and features: TODO (later labs)
- Leakage risk: columns known only after departure or arrival (`DepTime`, `DepDelay`, `DepDelayMinutes`, `DepDel15`, `TaxiOut`, `WheelsOff`, `ArrTime`, `ArrDelay`, `ArrDelayMinutes`) are not used as inputs.

## Resource use and contributions

- Cost or performance choice: TODO (for example Parquet size against the 1.4 GB CSV; Cool tier rule for old raw data)
- Cleanup: trigger stopped, Data flow debug off, VM stopped (deallocated); lake and factory kept.
- Contributions: TODO
- Reused code: ingestion helper adapted from the Lab 3 package (`tools/data.py`); ADF steps follow the Lab 4 guide, adapted from reviews JSON to flights CSV.

## AI use

| Tool | What I asked | What it returned | What I changed |
|---|---|---|---|
| Claude | Support and refute statements for H1 | Gains of 0.05 and 0.02 | Added them to the statements |
| Claude | Models for H1 and baseline | GBT and logistic regression | Put them in the report |
| Claude | Help adapting Lab 3 and Lab 4 to my flight data | Step-by-step commands, `tools/flights.py` and `tools/check_bronze.py` | TODO (what I edited, e.g. fixed the DuckDB call and removed the duplicate output) |
