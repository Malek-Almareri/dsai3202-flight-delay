#!/usr/bin/env python3
"""Phase 1 / Lab 7: build the feature table that the feature store points at.

Replaces the Lab-7 add_event_ts.py for the flight project. It does four things:
  1. reads the Lab-6 features_v1 rows of one published run (completed, validated flights);
  2. adds `flight_key` (the row id) and `prediction_ts` (the clock: scheduled departure hour minus 2 hours);
  3. adds history features computed ONLY from the 7 days before the flight date (point in time),
     plus scheduled traffic at the origin from the full bronze schedule;
  4. also adds the "naive" whole-period rates (a first attempt that leaks) so the leakage trap can be shown.
Then it checks its own work and writes one Parquet file plus a manifest.

Usage (on the VM, venv active):
  python3 tools/build_features.py --features "work/features_run/*.parquet" \
      --bronze "work/bronze/*/*.parquet" --expected-rows 4000000 --run-id 123456789
"""
import argparse, hashlib, json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--features", required=True, help="glob of the Lab-6 features_v1 Parquet part files")
ap.add_argument("--bronze", required=True, help="glob of the bronze Parquet files (every scheduled flight, incl. cancelled)")
ap.add_argument("--expected-rows", type=int, required=True, help="gold_rows from that run's success.json")
ap.add_argument("--expected-bronze", type=int, default=4078318, help="raw CSV data rows")
ap.add_argument("--run-id", default="", help="Lab-6 job run ID the features come from")
ap.add_argument("--out", default="work/features_ts")
ap.add_argument("--evidence", default="evidence/lab7")
ap.add_argument("--sample-mod", type=int, default=4000, help="audit about 1 row in N")
a = ap.parse_args()

MIN_N_ORIGIN_CARRIER = 30     # history needs at least this many flights in the window, else the feature is empty
MIN_N_ROUTE = 10
WINDOW_DAYS = 7               # history window: the 7 days BEFORE the flight date (never the flight date itself)

t0 = time.time()
con = duckdb.connect()
con.execute("SET memory_limit='1500MB'")
Path("work/duck_tmp").mkdir(parents=True, exist_ok=True)
con.execute("SET temp_directory='work/duck_tmp'")
fails = []

def one(sql):
    return con.execute(sql).fetchone()[0]

# 1. inputs ------------------------------------------------------------------------------------------------
n_feat = one(f"SELECT count(*) FROM read_parquet('{a.features}')")
n_bronze = one(f"SELECT count(*) FROM read_parquet('{a.bronze}')")
print(f"features_v1 rows : {n_feat}  (success.json says {a.expected_rows})")
print(f"bronze rows      : {n_bronze}  (raw CSV has {a.expected_bronze})")
if n_feat != a.expected_rows:
    sys.exit("STOP: the downloaded features do not match the run's gold row count.")
if n_bronze != a.expected_bronze:
    sys.exit("STOP: bronze does not match the raw CSV row count. Download it again and run tools/check_bronze.py.")

# 2. row id and clock ----------------------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE base AS
SELECT
  strftime(flight_date, '%Y-%m-%d') || '|' || carrier || '|' || CAST(flight_number AS VARCHAR) || '|' || origin
    || '|' || dest || '|' || CAST(dep_hour AS VARCHAR)                                  AS flight_key,
  timezone('UTC', CAST(flight_date AS TIMESTAMP) + to_hours(CAST(dep_hour AS BIGINT)) - to_hours(2)) AS prediction_ts,
  flight_date, carrier, origin, dest, route, dep_hour, arr_hour, day_of_week, month, distance,
  sched_elapsed_min, is_weekend, data_split, label_arr_del15 AS label
FROM read_parquet('{a.features}')
""")
distinct_keys = one("SELECT count(DISTINCT flight_key) FROM base")
print(f"distinct flight_key: {distinct_keys}")
if distinct_keys != n_feat:
    sys.exit("STOP: flight_key is not unique. Add the scheduled departure minute to the Lab-6 features and rerun the job.")

# 3. scheduled traffic (known days ahead, so safe at prediction time) ---------------------------------------
con.execute(f"""
CREATE TABLE traffic AS
SELECT Origin AS origin, CAST(FlightDate AS DATE) AS flight_date, CAST((CRSDepTime // 100) % 24 AS BIGINT) AS dep_hour,
       count(*) AS origin_sched_flights_hour
FROM read_parquet('{a.bronze}')
GROUP BY 1, 2, 3
""")

# 4. point-in-time history: daily totals, then a window over the 7 days before each date -------------------------
def lagged(name, keycol):
    con.execute(f"""
    CREATE TABLE lag_{name} AS
    WITH d AS (SELECT {keycol} AS k, flight_date AS d, count(*) AS n, sum(label) AS dl FROM base GROUP BY 1, 2)
    SELECT k, d, sum(n) OVER w AS n7, sum(dl) OVER w AS dl7
    FROM d
    WINDOW w AS (PARTITION BY k ORDER BY d
                 RANGE BETWEEN INTERVAL {WINDOW_DAYS} DAYS PRECEDING AND INTERVAL 1 DAY PRECEDING)
    """)

lagged("origin", "origin")
lagged("carrier", "carrier")
lagged("route", "route")

# 5. the feature table -----------------------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE feat AS
SELECT
  b.flight_key, b.prediction_ts,
  b.carrier, b.origin, b.dest, b.route,
  CAST(b.dep_hour AS BIGINT) AS dep_hour, CAST(b.arr_hour AS BIGINT) AS arr_hour,
  CAST(b.day_of_week AS BIGINT) AS day_of_week, CAST(b.month AS BIGINT) AS month,
  CAST(year(b.flight_date) AS BIGINT) AS year,
  CAST(b.distance AS DOUBLE) AS distance, CAST(b.sched_elapsed_min AS DOUBLE) AS sched_elapsed_min,
  b.is_weekend,
  t.origin_sched_flights_hour,
  CASE WHEN lo.n7 >= {MIN_N_ORIGIN_CARRIER} THEN lo.dl7 * 1.0 / lo.n7 END AS origin_delay_rate_7d,
  CASE WHEN lc.n7 >= {MIN_N_ORIGIN_CARRIER} THEN lc.dl7 * 1.0 / lc.n7 END AS carrier_delay_rate_7d,
  CASE WHEN lr.n7 >= {MIN_N_ROUTE} THEN lr.dl7 * 1.0 / lr.n7 END AS route_delay_rate_7d,
  avg(b.label) OVER (PARTITION BY b.origin)  AS origin_delay_rate_all,
  avg(b.label) OVER (PARTITION BY b.carrier) AS carrier_delay_rate_all,
  avg(b.label) OVER (PARTITION BY b.route)   AS route_delay_rate_all,
  b.flight_date, b.data_split, CAST(b.label AS BIGINT) AS label
FROM base b
LEFT JOIN traffic t      ON t.origin = b.origin AND t.flight_date = b.flight_date AND t.dep_hour = b.dep_hour
LEFT JOIN lag_origin lo  ON lo.k = b.origin  AND lo.d = b.flight_date
LEFT JOIN lag_carrier lc ON lc.k = b.carrier AND lc.d = b.flight_date
LEFT JOIN lag_route lr   ON lr.k = b.route   AND lr.d = b.flight_date
""")
n_out = one("SELECT count(*) FROM feat")
print(f"feature rows     : {n_out}")
if n_out != n_feat:
    sys.exit("STOP: the joins changed the row count (duplicate keys in a lookup table).")
if one("SELECT count(*) FROM feat WHERE origin_sched_flights_hour IS NULL") > 0:
    fails.append("origin_sched_flights_hour has empty values")

# 6. checks ---------------------------------------------------------------------------------------------------
print("\n-- point-in-time audit: recompute each history feature the slow, obvious way for a sample of rows --")
for name, col, minn, keycol in [("origin", "origin_delay_rate_7d", MIN_N_ORIGIN_CARRIER, "origin"),
                                ("carrier", "carrier_delay_rate_7d", MIN_N_ORIGIN_CARRIER, "carrier"),
                                ("route", "route_delay_rate_7d", MIN_N_ROUTE, "route")]:
    checked, bad = con.execute(f"""
    WITH s AS (SELECT flight_key, {keycol} AS k, flight_date, {col} AS got FROM feat WHERE hash(flight_key) % {a.sample_mod} = 0),
    w AS (
      SELECT s.flight_key, s.got,
             count(x.label) AS n, avg(x.label) AS want, max(x.flight_date) AS newest
      FROM s LEFT JOIN base x
        ON x.{keycol} = s.k AND x.flight_date >= s.flight_date - INTERVAL {WINDOW_DAYS} DAYS AND x.flight_date < s.flight_date
      GROUP BY s.flight_key, s.got
    )
    SELECT count(*),
           sum(CASE WHEN (got IS NULL) != (n < {minn}) OR (got IS NOT NULL AND abs(got - want) > 1e-9) THEN 1 ELSE 0 END)
    FROM w
    """).fetchone()
    newest_ok = one(f"""
    SELECT count(*) FROM (SELECT s.flight_key FROM feat s JOIN base x ON x.{keycol} = s.{keycol}
       AND x.flight_date >= s.flight_date - INTERVAL {WINDOW_DAYS} DAYS AND x.flight_date >= s.flight_date
       WHERE hash(s.flight_key) % {a.sample_mod} = 0 LIMIT 1) WHERE false""")
    print(f"{col:<24} sampled {checked} rows, mismatches {int(bad or 0)}")
    if bad:
        fails.append(f"{col}: {int(bad)} sampled rows differ from the independent recomputation")

print("\n-- empty history by month (the first days of January have no past) --")
for m, n, e1, e2, e3 in con.execute("""
    SELECT month, count(*), round(avg((origin_delay_rate_7d IS NULL)::INT), 4),
           round(avg((carrier_delay_rate_7d IS NULL)::INT), 4), round(avg((route_delay_rate_7d IS NULL)::INT), 4)
    FROM feat GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"month {m}: rows {n:>8}  empty origin {e1}  carrier {e2}  route {e3}")

print("\n-- constant columns (a constant teaches a model nothing) --")
for c in ["year", "carrier", "origin", "dest", "dep_hour", "day_of_week", "month", "distance", "is_weekend"]:
    d = one(f"SELECT count(DISTINCT {c}) FROM feat")
    print(f"{c:<12} distinct values: {d}" + ("   <-- CONSTANT" if d == 1 else ""))

print("\n-- correlation with the target (label = arrived 15+ minutes late) --")
corrs = {}
for c in ["origin_delay_rate_7d", "carrier_delay_rate_7d", "route_delay_rate_7d",
          "origin_delay_rate_all", "carrier_delay_rate_all", "route_delay_rate_all", "origin_sched_flights_hour", "dep_hour"]:
    corrs[c] = one(f"SELECT corr({c}, label) FROM feat")
    print(f"{c:<28} {corrs[c]:.4f}")

# 7. write --------------------------------------------------------------------------------------------------
out = Path(a.out)
out.mkdir(parents=True, exist_ok=True)
target = out / "features_ts.parquet"
if target.exists():
    sys.exit(f"STOP: {target} already exists. Use a new --out folder; saved features are never overwritten.")
con.execute(f"COPY (SELECT * FROM feat ORDER BY prediction_ts, flight_key) TO '{target}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{target}')").fetchall()
print("\nwrote", target, f"{one(f'SELECT count(*) FROM read_parquet({chr(39)}{target}{chr(39)})')} rows, {len(schema)} columns,",
      f"{target.stat().st_size / 1e6:.1f} MB, {time.time() - t0:.0f}s")
ts_type = [t for n, t, *_ in schema if n == "prediction_ts"][0]
print("prediction_ts is", ts_type)
if "TIME ZONE" not in ts_type.upper():
    fails.append(f"prediction_ts must be a timestamp with time zone, got {ts_type}")

h = hashlib.sha256()
with open(target, "rb") as f:
    for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
        h.update(chunk)

Path(a.evidence).mkdir(parents=True, exist_ok=True)
manifest = {
    "built_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "lab6_run_id": a.run_id, "rows": n_out, "columns": [{"name": n, "type": t} for n, t, *_ in schema],
    "file": target.name, "bytes": target.stat().st_size, "sha256": h.hexdigest(),
    "inputs": {"features": a.features, "bronze": a.bronze, "expected_rows": a.expected_rows},
    "history_window_days": WINDOW_DAYS, "min_flights": {"origin_carrier": MIN_N_ORIGIN_CARRIER, "route": MIN_N_ROUTE},
    "correlations": corrs, "problems": fails,
}
Path(a.evidence, "features_ts_manifest.json").write_text(json.dumps(manifest, indent=2))
print("manifest:", Path(a.evidence, "features_ts_manifest.json"))

if fails:
    print("\nPROBLEMS:")
    for f_ in fails:
        print(" -", f_)
    sys.exit(1)
print("\nFEATURES BUILT: all checks passed")
