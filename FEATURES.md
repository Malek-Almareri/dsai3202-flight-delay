# Feature sets for flight arrival delay (Phase 1)

Prediction moment: **two hours before the scheduled departure** of a US domestic flight.
Target: `label` = 1 if the flight arrives 15 or more minutes late, else 0.
Table: `curated/flights/combined_flights_2022/v1/features_ts/features_ts.parquet`, built by `tools/build_features.py`
from the validated Lab-6 features (completed flights only). Manifest with hash: `evidence/lab7/features_ts_manifest.json`.
Registered in the Azure ML feature store as entity `Flight` v1 and feature set `flight_features` v1 and v2. **Version 2 is the one to use.**

## Row id and clock

| Column | Type | Definition |
|---|---|---|
| `flight_key` | string | `date|carrier|flight number|origin|dest|scheduled departure hour`, unique per row (checked on every build) |
| `prediction_ts` | timestamp (UTC) | scheduled departure hour minus 2 hours. The dataset has no time zones, so local clock times are labelled UTC. The hour is rounded down, so the cutoff is never later than the true one. |

## Version 2 features (15)

| Feature | Type | Definition | Source | Known at prediction time? |
|---|---|---|---|---|
| `carrier` | string | Operating airline code | `Operating_Airline` | Yes, published schedule |
| `origin`, `dest` | string | Airport codes | `Origin`, `Dest` | Yes |
| `route` | string | `origin-dest` | `Origin`, `Dest` | Yes |
| `dep_hour`, `arr_hour` | long | Scheduled hour, 0 to 23 | `CRSDepTime`, `CRSArrTime` | Yes |
| `day_of_week` | long | 1 = Monday to 7 = Sunday | `DayOfWeek` | Yes |
| `month` | long | 1 to 7 | `Month` | Yes |
| `distance` | double | Miles | `Distance` | Yes |
| `sched_elapsed_min` | double | Scheduled gate-to-gate minutes | `CRSElapsedTime` | Yes |
| `is_weekend` | boolean | Saturday or Sunday | `DayOfWeek` | Yes |
| `origin_sched_flights_hour` | long | Scheduled flights leaving the same origin in the same hour, all flights including cancelled | bronze schedule | Yes: schedules are set days ahead, and no delay outcome is used |
| `origin_delay_rate_7d` | double | Share of completed flights from this origin that arrived 15+ minutes late, over the 7 days **before** the flight date. Empty if fewer than 30 flights. | validated flights | Yes: only earlier dates are used |
| `carrier_delay_rate_7d` | double | Same, by carrier. Empty if fewer than 30 flights. | validated flights | Yes |
| `route_delay_rate_7d` | double | Same, by route. Empty if fewer than 10 flights. | validated flights | Yes |

Carried alongside (not model inputs): `flight_key`, `prediction_ts`, `flight_date`, `data_split` (`train` Jan to Apr, `validation` May, `test` Jun, `deploy` Jul), `label`.

## Version 1 to version 2: what changed and why

Version 1 was the first list of 16 candidates. It is kept unchanged as a record. Each candidate was tested with the clock test (does the value exist before prediction time?) and a variation test (does it change between rows?).

| Candidate (in v1 only) | Problem | Result |
|---|---|---|
| `year` | The same value, 2022, in every row. It teaches a model nothing. | Removed in v2 |
| `origin_delay_rate_all`, `carrier_delay_rate_all`, `route_delay_rate_all` | Average over the **whole** period. Includes the flight's own outcome and flights after it, so it uses the future. Worst for rare routes. | Replaced in v2 by the `_7d` versions above |

The build script proves the `_7d` columns are point in time: for a sample of rows it recomputes each value from scratch using only dates before the flight date, and the build fails on any mismatch.

## Not used as inputs (leakage)

| Column(s) | Why excluded |
|---|---|
| `DepTime`, `DepDelay`, `DepDelayMinutes`, `DepDel15`, `DepartureDelayGroups` | Only known once the flight has left |
| `TaxiOut`, `WheelsOff`, `WheelsOn`, `TaxiIn` | Recorded during the flight |
| `ArrTime`, `ArrDelay`, `ArrDelayMinutes`, `ArrivalDelayGroups`, `AirTime`, `ActualElapsedTime` | Describe the outcome itself |
| `Cancelled`, `Diverted`, `DivAirportLandings` | Known only after the fact; used for cleaning, not as inputs |

## Known limits

* No weather data. Covers January to July 2022 only.
* The first 7 days of January have no history, and sparse routes have empty `route_delay_rate_7d`. The empty share is reported by month on every build. A model must handle empty values.
* History uses whole previous calendar days. Delays earlier on the same day (cascading delays) would help but need exact timestamps and time zones. That is a possible future version, not v2.
* History counts completed flights only, because cancelled and diverted flights are removed in cleaning.
* Aggregates over previous days can still hide a real change in conditions (a storm week). That is a property of the data, not a leak.
* Categories such as `origin` and `route` have many values; encoding them is a modeling decision for later.
