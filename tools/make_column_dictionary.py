#!/usr/bin/env python3
"""Write COLUMNS.md: the column dictionary of the published flight table (gold_flights).

Types and null counts are measured, not typed by hand:
  * types come from the bronze Parquet files, which have exactly the columns and types of gold_flights;
  * gold_flights = bronze rows that are not cancelled or diverted (and pass the cleaning rules, which rejected 0 rows),
    so the same filter on bronze reproduces gold, apart from the rows quarantined later. The script checks that count.

Usage: python3 tools/make_column_dictionary.py --bronze "work/bronze/*/*.parquet" --expected-accepted 3944916 --out COLUMNS.md
"""
import argparse, sys
import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--bronze", required=True)
ap.add_argument("--expected-accepted", type=int, required=True,
                help="gold rows + quarantined rows of the run (accepted rows)")
ap.add_argument("--out", default="COLUMNS.md")
a = ap.parse_args()

# role: key | target | pre | post | outcome | flag | id | split
D = {
 "FlightDate": ("key", "Date of the flight (local date)"),
 "Airline": ("pre", "Operating airline name"),
 "Origin": ("key", "Origin airport code"),
 "Dest": ("key", "Destination airport code"),
 "Cancelled": ("flag", "True if the flight was cancelled. Used for cleaning; such flights are excluded from gold"),
 "Diverted": ("flag", "True if the flight was diverted. Used for cleaning; such flights are excluded from gold"),
 "CRSDepTime": ("key", "Scheduled departure time, local, HHMM"),
 "DepTime": ("post", "Actual departure time, HHMM"),
 "DepDelayMinutes": ("post", "Departure delay in minutes, early departures counted as 0"),
 "DepDelay": ("post", "Departure delay in minutes, negative if early"),
 "ArrTime": ("outcome", "Actual arrival time, HHMM"),
 "ArrDelayMinutes": ("outcome", "Arrival delay in minutes, early arrivals counted as 0. Used to check the target"),
 "AirTime": ("post", "Minutes in the air"),
 "CRSElapsedTime": ("pre", "Scheduled gate-to-gate minutes"),
 "ActualElapsedTime": ("outcome", "Actual gate-to-gate minutes"),
 "Distance": ("pre", "Distance in miles"),
 "Year": ("pre", "Calendar year"),
 "Quarter": ("pre", "Calendar quarter"),
 "Month": ("split", "Calendar month 1 to 7. Partition column of silver and gold"),
 "DayofMonth": ("pre", "Day of the month"),
 "DayOfWeek": ("pre", "1 = Monday to 7 = Sunday"),
 "Marketing_Airline_Network": ("pre", "Marketing carrier code"),
 "Operated_or_Branded_Code_Share_Partners": ("pre", "Code-share partner information"),
 "DOT_ID_Marketing_Airline": ("id", "US DOT identifier of the marketing carrier"),
 "IATA_Code_Marketing_Airline": ("pre", "IATA code of the marketing carrier"),
 "Flight_Number_Marketing_Airline": ("id", "Flight number under the marketing carrier"),
 "Operating_Airline": ("key", "IATA code of the operating carrier"),
 "DOT_ID_Operating_Airline": ("id", "US DOT identifier of the operating carrier"),
 "IATA_Code_Operating_Airline": ("pre", "IATA code of the operating carrier"),
 "Tail_Number": ("pre", "Aircraft registration. The assigned aircraft can change close to departure, so it is not used"),
 "Flight_Number_Operating_Airline": ("key", "Flight number under the operating carrier"),
 "OriginAirportID": ("id", "US DOT airport identifier, origin"),
 "OriginAirportSeqID": ("id", "US DOT airport sequence identifier, origin"),
 "OriginCityMarketID": ("id", "US DOT city market identifier, origin"),
 "OriginCityName": ("pre", "Origin city and state"),
 "OriginState": ("pre", "Origin state code"),
 "OriginStateFips": ("id", "Origin state FIPS code"),
 "OriginStateName": ("pre", "Origin state name"),
 "OriginWac": ("id", "Origin world area code"),
 "DestAirportID": ("id", "US DOT airport identifier, destination"),
 "DestAirportSeqID": ("id", "US DOT airport sequence identifier, destination"),
 "DestCityMarketID": ("id", "US DOT city market identifier, destination"),
 "DestCityName": ("pre", "Destination city and state"),
 "DestState": ("pre", "Destination state code"),
 "DestStateFips": ("id", "Destination state FIPS code"),
 "DestStateName": ("pre", "Destination state name"),
 "DestWac": ("id", "Destination world area code"),
 "DepDel15": ("post", "1 if the departure was 15 or more minutes late"),
 "DepartureDelayGroups": ("post", "Departure delay in 15-minute groups"),
 "DepTimeBlk": ("pre", "Scheduled departure hour block"),
 "TaxiOut": ("post", "Taxi-out minutes"),
 "WheelsOff": ("post", "Wheels-off time, HHMM"),
 "WheelsOn": ("post", "Wheels-on time, HHMM"),
 "TaxiIn": ("post", "Taxi-in minutes"),
 "CRSArrTime": ("pre", "Scheduled arrival time, local, HHMM"),
 "ArrDelay": ("outcome", "Arrival delay in minutes, negative if early"),
 "ArrDel15": ("target", "TARGET. 1 if the flight arrived 15 or more minutes late, else 0"),
 "ArrivalDelayGroups": ("outcome", "Arrival delay in 15-minute groups"),
 "ArrTimeBlk": ("pre", "Scheduled arrival hour block"),
 "DistanceGroup": ("pre", "Distance in 250-mile groups"),
 "DivAirportLandings": ("post", "Number of landings at diversion airports"),
 "data_split": ("split", "train (Jan to Apr), validation (May), test (Jun) or deploy (Jul). Added by the ADF data flow from Month"),
}
ROLE = {"key": "key", "target": "target", "pre": "known before departure", "post": "AFTER departure (leakage)",
        "outcome": "outcome (leakage)", "flag": "cleaning flag", "id": "identifier", "split": "split / partition"}

con = duckdb.connect()
src = f"read_parquet('{a.bronze}', hive_partitioning=false)"
cols = [(n, t) for n, t, *_ in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
total = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
accepted = con.execute(f"SELECT count(*) FROM {src} WHERE Cancelled = false AND Diverted = false").fetchone()[0]
print(f"bronze rows {total} | not cancelled and not diverted {accepted} | expected accepted {a.expected_accepted}")
if accepted != a.expected_accepted:
    sys.exit("STOP: the filter does not reproduce the accepted rows, so null counts would describe the wrong rows.")

sel = ", ".join(f'count(*) - count("{n}") AS "{n}"' for n, _ in cols)
nulls = dict(zip([n for n, _ in cols], con.execute(f"SELECT {sel} FROM {src} WHERE Cancelled = false AND Diverted = false").fetchone()))

unknown = [n for n, _ in cols if n not in D]
lines = ["# Column dictionary: gold_flights", "",
         f"{len(cols)} columns, 3,944,915 rows after cleaning (generated by `tools/make_column_dictionary.py`; types read from the data).",
         f"Empty values are counted over the {accepted:,} accepted rows (bronze rows that are not cancelled or diverted); gold also leaves out 1 quarantined row.", "",
         "Roles: **key** (part of the row key), **target**, **known before departure** (allowed as a model input), "
         "**AFTER departure / outcome** (never a model input), **cleaning flag**, **identifier**, **split / partition**.", "",
         "| # | Column | Type | Role | Empty values | Description |", "|---|---|---|---|---|---|"]
for i, (n, t) in enumerate(cols, 1):
    role, desc = D.get(n, ("?", "(no description yet)"))
    lines.append(f"| {i} | `{n}` | {t} | {ROLE.get(role, role)} | {nulls[n]:,} | {desc} |")
open(a.out, "w").write("\n".join(lines) + "\n")
print("wrote", a.out, "|", len(cols), "columns | without description:", unknown or "none")
over = [(n, v) for n, v in nulls.items() if v > 0]
print("columns with empty values in accepted rows:", over or "none")
