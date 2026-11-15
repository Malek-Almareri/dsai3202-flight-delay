# Leakage check for flight_features v1

Prediction moment: 2 hours before the scheduled departure of a flight.
Tests: (1) clock test, is the value known by that moment? (2) variation test, does it change between rows?
Variation facts come from the distinct-value counts in `evidence/lab7/build.txt`.

| Feature | Known before / at / after? | Varies between rows? | Keep? | Reason |
|---|---|---|---|---|
| carrier | before | yes | keep | Airline code from the published schedule |
| origin | before | yes | keep | Airport from the published schedule |
| dest | before | yes | keep | Airport from the published schedule |
| route | before | yes | keep | Built from origin and dest, both in the schedule |
| dep_hour | before | yes | keep | Scheduled departure hour, set in the schedule |
| arr_hour | before | yes | keep | Scheduled arrival hour, set in the schedule (not the actual arrival) |
| day_of_week | before | yes | keep | Derived from the flight date |
| month | before | yes | keep | Derived from the flight date |
| year | before | no, always 2022 | remove | Same value in every row, so it teaches a model nothing |
| distance | before | yes | keep | Fixed property of the route |
| sched_elapsed_min | before | yes | keep | Scheduled (planned) time, not the actual time flown |
| is_weekend | before | yes | keep | Derived from the flight date |
| origin_sched_flights_hour | before | yes | keep | Flights scheduled at the origin that hour; schedules are set days ahead and no delay outcome is used |
| origin_delay_rate_all | after | yes | remove | Average over the whole period: includes this flight's own outcome and flights after it |
| carrier_delay_rate_all | after | yes | remove | Same problem, by carrier |
| route_delay_rate_all | after | yes | remove | Same problem, by route; worst on rare routes where one flight carries most of the average |

## Decision

Version 2 removes `year` (constant) and the three `*_delay_rate_all` features (they use the future).
It replaces the rates with `origin_delay_rate_7d`, `carrier_delay_rate_7d` and `route_delay_rate_7d`, each computed only from
the 7 days before the flight date. `tools/build_features.py` recomputes a sample of these values from scratch on every build
and fails if any differ (see the point-in-time audit in `evidence/lab7/build.txt`).

Version 1 stays registered, unchanged, as a record of the first attempt.
