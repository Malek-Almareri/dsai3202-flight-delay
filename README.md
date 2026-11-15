# DSAI 3202 Phase 1: US flight arrival delay data foundation

Malek Al-Mareri (60303551), Group 3 · Submitted commit: tag `phase1-submission` · Repository: this one

## Project and hypothesis

* **Task:** predict whether a scheduled US domestic flight arrives 15 or more minutes late (`ArrDel15 = 1`), two hours before departure. Users: airline operations teams, who act early on high-risk flights.
* **Baseline plan:** logistic regression on carrier, origin, destination, scheduled hour, day of week and distance, plus an always-predict-the-average-rate reference.
* **Hypothesis:** a gradient-boosted tree model with engineered features (origin traffic and recent delay rates by airport, carrier and route) beats the baseline by at least 0.05 PR-AUC on later, unseen flights. A gain under 0.02 refutes it; in between is partial support. F1 and ROC-AUC are also reported.
* **Split by date (2022):** train Jan–Apr, validation May, test Jun, deploy Jul.
* **Approved changes:** none. Two clarifications only: "recent" means the 7 days before the flight date, and cancelled and diverted flights are excluded because they have no arrival outcome.

## Dataset and architecture

* **Source:** Kaggle, [`Combined_Flights_2022.csv`]([https://www.kaggle.com/datasets/robikscube/flight-delay-dataset20182022](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2022.csv)) (version and download details in `evidence/source-manifest.json`). 1,415,293,445 bytes (1.42 GB), 61 columns, 4,078,318 rows, January to July 2022. Not a course-lab dataset. The full file is ingested and profiled; no sample is used.
* **Ingestion:** batch, manual, one static snapshot. A new snapshot would go to `v2/` with its own manifest. Raw is never overwritten (`--overwrite=false`). SHA-256 `b4cde15b…7824a3` identifies the file. The lake copy of `_manifest.json` was uploaded before the Kaggle version was added and is never overwritten; the repository copy is the complete one, with the same hash and size. The lake copy of _manifest.json was uploaded before the Kaggle version was added and is never overwritten; the repository copy is the complete one, with the same hash and size.
* **Schema, keys, assumptions and lineage:** [CATALOG.md](CATALOG.md) and [COLUMNS.md](COLUMNS.md). Tables are registered in Unity Catalog (`dsai3202_databricks.s_60303551.gold_flights`).

```mermaid
flowchart LR
  L["Laptop: Kaggle CSV"] -->|"scp, AzCopy"| VM["Azure VM: scripts and checks"]
  VM --> RAW[("ADLS Gen2: raw")]
  RAW -->|"Data Factory data flow"| BR[("processed: bronze by month")]
  BR -->|"Databricks job: clean, validate, publish"| GD[("curated: gold_flights, Unity Catalog")]
  GD --> FT[("curated: features_ts")]
  FT --> FS["Azure ML feature store"]
```

| Service | Role |
|---|---|
| ADLS Gen2 | Storage: `raw/`, `processed/`, `curated/` under `flights/combined_flights_2022/v1/` |
| Azure VM | Runs ingestion, checks and the feature build (Python, Azure CLI, AzCopy, DuckDB) |
| Data Factory | Converts raw CSV to typed Parquet in month folders (adds `data_split`) |
| Databricks + Unity Catalog | Job `clean → validate → publish`, Delta table `gold_flights`, catalog entry |
| Azure ML feature store | Versioned feature definitions (entity `Flight`, feature set `flight_features` v1 and v2) |

Layout: `raw/…/v1/` CSV + `_manifest.json` · `processed/…/v1/bronze/month=01…07/` · `curated/…/v1/runs/<run_id>/` (accepted, rejects, `features_v1`) · `curated/…/v1/gold_flights/` · `curated/…/v1/features_ts/` · `curated/…/v1/quality/` (run records).

## Run the pipeline

**Prerequisites:** an Azure subscription; an ADLS Gen2 account with `raw`, `processed`, `curated`; Data Factory whose managed identity has *Storage Blob Data Contributor* on it; a Databricks workspace with a Unity Catalog schema and a cluster; an Azure ML feature store; a VM with Python 3, Azure CLI and AzCopy. No credentials are stored in this repository; sign in with `az login`. The CSV is not in Git: download it from Kaggle and upload it as below.

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
source ~/.config/dsai3202/lab3.env; export SA RG LOC           # lake name and resource group
python3 tools/flights.py manifest <sha256-of-your-csv>         # source receipt; then: inspect
azcopy copy work/source/Combined_Flights_2022.csv "https://$SA.blob.core.windows.net/raw/flights/combined_flights_2022/v1/Combined_Flights_2022.csv" --overwrite=false
python3 tools/flights.py verify                                 # re-downloads and compares hashes
```
1. **Data Factory (portal-built):** recreate `adf/*.json` (linked service, 3 datasets, 2 pipelines, data flow, trigger) and run `pl_transform_flights` once with *Trigger now*. Re-running needs the `bronze/` folder deleted first. Check with `python3 tools/check_bronze.py`.
2. **Databricks:** import `databricks/lab6/*.py`; run `05_make_test_input` once; create the job from `databricks/lab6/job-export.json` (tasks `clean`, `validate`, `publish`; parameters lake, catalog, schema, version, `run_id={{job.run_id}}`, input_dir, expected_rows); *Run now*.
3. **Features:** `python3 tools/build_features.py --features "work/features_run/*.parquet" --bronze "work/bronze/*/*.parquet" --expected-rows <gold_rows> --run-id <job run id>`, upload `features_ts.parquet`, `python3 tools/check_spec.py spec/…/FeatureSetSpec.yaml "work/features_ts/*.parquet" <rows>`, then `az ml feature-set create --file spec/featureset_v2.yml …`.
4. **Documents:** `python3 tools/make_column_dictionary.py --bronze "work/bronze/*/*.parquet" --expected-accepted 3944916`.

## Results and features

**Row accounting (every row is explained):** 4,078,318 raw = 133,402 excluded (cancelled or diverted) + 0 invalid + 0 duplicates + 1 quarantined + **3,944,915 gold**.

| Check | Result | Evidence |
|---|---|---|
| Ingestion identity | Source, raw copy and manifest hashes match | `evidence/verify-1.txt` |
| Repeat ingestion | Second upload skipped, receipts unchanged | `evidence/retry-*.txt`, `evidence/receipts-*.tsv` |
| Parquet copy | 4,078,318 rows in and out, hashes match in Azure | `evidence/convert.txt`, `evidence/publish-check.txt` |
| Bronze vs raw | Row count equal to the raw CSV (asserted by every `clean` run); month by month in the Lab-5 notebook | `evidence/lab6/<run>/clean.json`, `databricks/lab5/` |
| Successful runs | Job runs `431312011309625` and `642041215727604` | `evidence/lab6/` |
| Safe rerun | The two runs hold identical gold and features (Delta versions 0 and 1) | `evidence/lab6/04_inspect_flights.html` |
| Controlled failure | Run `445882245804967` on a separate test input (81,473 rows, May labels all delayed): gate stopped it on `label_rate_by_month`; no publish, no features, gold unchanged | `gate.json`, same notebook |

**Quality gate (stops publication):** required columns and types, at least 1,000 rows, all 7 months present, delay rate 10–35% overall and 8–40% per month, no duplicate keys, at least 99% of non-excluded rows kept, quarantine under 2%. Rows with no scheduled elapsed time are quarantined; month imbalance and train/test rate shift only warn. Cleaning rules are listed in [CATALOG.md](CATALOG.md).

**EDA** (full table, no sample; [EDA.md](EDA.md)): 21.65% of flights are late. The rate climbs from 9.0% at 05:00 to 31.7% at 21:00. Post-departure `DepDel15` correlates 0.75 with the target, while every schedule feature stays under 0.16, so those columns are excluded and a modest gain from schedule features is expected. The test month has more late flights (23.9%) than training (20.7%). Cleaning keeps 92.8% to 99.1% of each carrier's flights.

**Features** ([FEATURES.md](FEATURES.md)): 15 features known two hours before departure: carrier, origin, destination, route, scheduled departure and arrival hour, day of week, month, distance, scheduled elapsed time, weekend flag, scheduled flights at the origin that hour, and delay rates over the previous 7 days for origin, carrier and route. Version 1 (16 candidates) was kept as a record; it held a constant (`year`) and three whole-period delay rates that use future flights. Version 2 replaces them. A build audit recomputed 1,016 sampled rows per rate from scratch with 0 mismatches. Versioned output: `features_ts.parquet` (3,944,915 rows, SHA-256 `10f304d8…caae217`, `evidence/lab7/`) and feature sets v1 and v2.

## Resource use and contributions

* **Resource choice:** processed data is stored as Parquet, not CSV: 103,608,121 bytes against 1,415,293,445 (13.7 times smaller), so storage and every later read cost less. The whole Databricks job from cleaning to publication took 168.6 seconds (`success.json`).
* **Cleanup steps:** keep trigger `tr_daily_flights` stopped, turn Data flow debug off, terminate the Databricks compute, stop (deallocate) the VM; keep the lake, factory and feature store; delete the GitHub token after submission.
* **Contributions:** Malek Al-Mareri: everything in this repository (ingestion, ETL, quality gate, features, documents). NAME: WHAT THEY DID; NAME: WHAT THEY DID.
* **Reused code:** adapted from the course lab packages: Lab 3 `data.py` → `tools/flights.py`; Lab 4 ADF guide → `adf/`; Lab 5 `01_silver.py` → `databricks/lab5/`; Lab 6 job notebooks → `databricks/lab6/`; Lab 7 `add_event_ts.py` → `tools/build_features.py`. Changes: flight-specific cleaning rules, keys and gate thresholds, and point-in-time history features built with DuckDB.

## AI use

| Tool | What I asked | What it returned | What I changed |
|---|---|---|---|
| Claude | Support and refute statements for the hypothesis; models for H1 and baseline | Gains of 0.05 and 0.02; gradient-boosted trees and logistic regression | Added them to the hypothesis and report |
| Claude | Adapt Labs 3 to 7 to my flight data: ingestion, ADF, Databricks job, features, catalog and EDA documents | Step-by-step commands, scripts, notebooks, specs and drafts (`tools/`, `databricks/`, `spec/`, `CATALOG.md`, `EDA.md`, `FEATURES.md`) | Ran every step on my own Azure resources, reported errors and applied the fixes (DuckDB call, Spark save mode, cluster reference, folder paths), and checked results against raw counts and hashes |
| Claude | Draft this README | A draft built from my run outputs | Checked each number against the evidence files |
