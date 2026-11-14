# Feature set v1 (schedule-only)

Prediction moment: **two hours before the scheduled departure** of a US domestic flight.
Target: `label_arr_del15` = 1 if the flight arrives 15 or more minutes late, else 0.
Built by `make_features()` in `00_common_flights.py`; saved per run at `curated/flights/combined_flights_2022/v1/runs/<run_id>/features_v1/` (Parquet).
Rows come from the validated gold table (completed flights only; cancelled and diverted flights are excluded by the task definition).

## Features

| Feature | Type | Definition | Source column(s) | Known at prediction time? |
|---|---|---|---|---|
| `carrier` | string | Operating airline code | `Operating_Airline` | Yes, published schedule |
| `origin` | string | Origin airport code | `Origin` | Yes |
| `dest` | string | Destination airport code | `Dest` | Yes |
| `route` | string | `origin-dest` | `Origin`, `Dest` | Yes |
| `dep_hour` | int | Scheduled departure hour, 0 to 23 | `CRSDepTime` (HHMM) | Yes |
| `arr_hour` | int | Scheduled arrival hour, 0 to 23 | `CRSArrTime` (HHMM) | Yes |
| `day_of_week` | int | 1 = Monday to 7 = Sunday | `DayOfWeek` | Yes |
| `month` | int | 1 to 7 (Jan to Jul 2022) | `Month` | Yes |
| `distance` | double | Distance in miles | `Distance` | Yes |
| `sched_elapsed_min` | double | Scheduled gate-to-gate minutes | `CRSElapsedTime` | Yes |
| `is_weekend` | boolean | Saturday or Sunday | `DayOfWeek` | Yes |

Carried alongside the features (not model inputs): `flight_date`, `flight_number` (row identity), `data_split` (`train` Jan to Apr, `validation` May, `test` Jun, `deploy` Jul), `label_arr_del15` (target).

## Not used as inputs (leakage)

These columns are recorded after departure or arrival, so using them would leak the answer.

| Column(s) | Why excluded |
|---|---|
| `DepTime`, `DepDelay`, `DepDelayMinutes`, `DepDel15`, `DepartureDelayGroups` | Only known once the flight has left |
| `TaxiOut`, `WheelsOff`, `WheelsOn`, `TaxiIn` | Recorded during the flight |
| `ArrTime`, `ArrDelay`, `ArrDelayMinutes`, `ArrivalDelayGroups`, `AirTime`, `ActualElapsedTime` | Describe the outcome itself |
| `Cancelled`, `Diverted`, `DivAirportLandings` | Known only after the fact; used for cleaning, not as inputs |

The inspect notebook (cell 2.3) prints each column's correlation with the target to show the size of this risk.

## Planned in the next lab (not in v1)

The engineered features from the hypothesis: scheduled traffic at the origin airport, and recent delay rates by airport, carrier and route.
Rule for these: for each flight, use only flights whose outcome was already known **before that flight's prediction time**. A rate computed over the whole period would leak the future.

## Known limits

* No weather data. Covers January to July 2022 only.
* `Operating_Airline` and `Origin`/`Dest` are high-cardinality categories; encoding is a modeling decision for later.
* Duplicate-key rule keeps the smallest full row; this is reproducible, not proof of which copy is correct.
