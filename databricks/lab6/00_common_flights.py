# Databricks notebook source
# MAGIC %md
# MAGIC # 00_common_flights · shared names, rules and helpers
# MAGIC Every task notebook starts with `%run ./00_common_flights`. Run this notebook once by hand to read it; the job never runs it as a task.
# MAGIC
# MAGIC Adapted from the Lab-6 package (00_common) for the flight project. Cleaning rules come from the Lab-5 flight notebook.
# MAGIC
# MAGIC | Job parameter | Meaning |
# MAGIC |---|---|
# MAGIC | lake, catalog, schema | Your lake name, `dsai3202_databricks`, and `s_YOUR_ID` (schema name only) |
# MAGIC | version | Dataset version folder, `v1` |
# MAGIC | run_id | `{{job.run_id}}`: names this run's folders, which are never overwritten |
# MAGIC | input_dir | `bronze` for the real data, `bronze_test` for the controlled failure |
# MAGIC | expected_rows | Raw CSV row count (4078318) for the real input; leave empty for the test input |

# COMMAND ----------
import json, time
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window as W

def _widget(name, default=""):
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()

LAKE = _widget("lake")
CATALOG = _widget("catalog", "dsai3202_databricks")
SCHEMA = _widget("schema")
VERSION = _widget("version", "v1")
RUN_ID = _widget("run_id")
INPUT_DIR = _widget("input_dir", "bronze")
EXPECTED_ROWS = _widget("expected_rows", "")

BASE = f"flights/combined_flights_2022/{VERSION}"

def lake_path(container, rel):
    return f"abfss://{container}@{LAKE}.dfs.core.windows.net/{rel}"

def run_paths(rid):
    return {"root": lake_path("curated", f"{BASE}/runs/{rid}"),
            "q": lake_path("curated", f"{BASE}/quality/lab6_runs/{rid}")}

INPUT = lake_path("processed", f"{BASE}/{INPUT_DIR}")
RUN_ROOT = run_paths(RUN_ID)["root"]
QDIR = run_paths(RUN_ID)["q"]
GOLD = lake_path("curated", f"{BASE}/gold_flights")
TABLE = f"{CATALOG}.{SCHEMA}.gold_flights"

# COMMAND ----------
# Small helpers. Saved run folders are never overwritten: a rerun needs a new run_id.
def exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

def save_json(path, obj):
    dbutils.fs.put(path, json.dumps(obj, indent=2, default=str), False)   # False = refuse to overwrite

def read_json(path):
    return json.loads(dbutils.fs.head(path, 1000000))

def require_run():
    missing = [n for n, v in {"lake": LAKE, "schema": SCHEMA, "run_id": RUN_ID}.items() if not v]
    if missing:
        raise Exception(f"Missing job parameter(s): {missing}")
    if "{" in RUN_ID:
        raise Exception("run_id still has braces. In the job parameters type {{job.run_id}} exactly.")

def latest_gold_version():
    try:
        from delta.tables import DeltaTable
        return int(DeltaTable.forPath(spark, GOLD).history(1).collect()[0]["version"])
    except Exception:
        return None

# COMMAND ----------
# Cleaning rules (Lab-5 notebook). Each row gets the FIRST rule it fails, so counts never overlap.
KEY = ["FlightDate", "Operating_Airline", "Flight_Number_Operating_Airline", "Origin", "Dest", "CRSDepTime"]
MAX_BAD_RATE = 0.01     # cleaning stops if invalid + duplicate rows exceed 1% of rows that were not excluded by design

def _blank(c):
    return F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")

def _bad_hhmm(c):
    return F.col(c).isNull() | (F.col(c) < 0) | (F.col(c) > 2400) | ((F.col(c) % 100) >= 60)

RULES = [
    ("missing_key",
     _blank("FlightDate") | _blank("Operating_Airline") | _blank("Origin") | _blank("Dest")
     | _blank("Flight_Number_Operating_Airline") | _blank("CRSDepTime")
     | F.col("Cancelled").isNull() | F.col("Diverted").isNull()),
    ("excluded_cancelled", F.col("Cancelled")),
    ("excluded_diverted", F.col("Diverted")),
    ("bad_label", F.col("ArrDel15").isNull() | ~F.col("ArrDel15").isin(0.0, 1.0)),
    ("date_out_of_range",
     (F.col("FlightDate") < F.lit("2022-01-01").cast("date")) | (F.col("FlightDate") > F.lit("2022-07-31").cast("date"))),
    ("month_mismatch", (F.month("FlightDate") != F.col("Month")) | (F.year("FlightDate") != F.col("Year"))),
    ("bad_schedule_time", _bad_hhmm("CRSDepTime") | _bad_hhmm("CRSArrTime")),
    ("bad_airport",
     (F.length(F.trim(F.col("Origin"))) != 3) | (F.length(F.trim(F.col("Dest"))) != 3) | (F.col("Origin") == F.col("Dest"))),
    ("bad_distance", F.col("Distance").isNull() | (F.col("Distance") <= 0)),
    ("label_inconsistent",
     F.col("ArrDelayMinutes").isNull()
     | ((F.col("ArrDel15") == 1) & (F.col("ArrDelayMinutes") < 15))
     | ((F.col("ArrDel15") == 0) & (F.col("ArrDelayMinutes") >= 15))),
]

def flag(df):
    reason = None
    for name, cond in RULES:
        reason = F.when(cond, name) if reason is None else reason.when(cond, name)
    return df.withColumn("reject_reason", reason)

def dedupe(valid, cols):
    """One row per KEY. Tie-break: smallest full payload (reproducible, not proof of truth)."""
    w = W.partitionBy(*KEY).orderBy(F.to_json(F.struct(*[F.col(c) for c in cols])).asc())
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    accepted = ranked.where("_rn = 1").drop("_rn", "reject_reason", "source_file")
    dups = ranked.where("_rn > 1").drop("_rn").withColumn("reject_reason", F.lit("duplicate_key"))
    return accepted, dups

# COMMAND ----------
# Quality gate rules. Policies: FAIL stops publication, QUARANTINE removes the rows and continues, WARN records a note.
REQUIRED_MONTHS = [1, 2, 3, 4, 5, 6, 7]
MIN_ROWS = 1000
OVERALL_RATE = (0.10, 0.35)      # share of delayed flights, whole run. Set from the full-data profile and justify in the README.
MONTH_RATE = (0.08, 0.40)        # share of delayed flights, each month
MIN_RETENTION = 0.99             # 1 - (invalid + duplicates) / rows not excluded by design
MAX_QUARANTINE_SHARE = 0.02
WARN_IMBALANCE = 0.70            # smallest month / largest month
WARN_SPLIT_SHIFT = 0.10          # |delay rate train - delay rate test|
EXPECTED_TYPES = {
    "FlightDate": {"date"}, "Cancelled": {"boolean"}, "Diverted": {"boolean"},
    "ArrDel15": {"double", "float"}, "Month": {"int", "bigint", "smallint"},
    "CRSDepTime": {"int", "bigint"}, "Distance": {"double", "float"},
    "Origin": {"string"}, "Dest": {"string"},
}

def quarantine_condition():
    """Rows that are kept for review but not published: no usable scheduled elapsed time (a model input)."""
    return F.col("CRSElapsedTime").isNull() | (F.col("CRSElapsedTime") <= 0)

def evaluate_gate(accepted, clean):
    n = accepted.count()
    results = []
    def add(rule, policy, passed, value, limit):
        results.append({"rule": rule, "policy": policy, "passed": bool(passed), "value": value, "limit": limit})

    types = dict(accepted.dtypes)
    bad_types = {c: types.get(c) for c, ok in EXPECTED_TYPES.items() if types.get(c) not in ok}
    add("schema", "FAIL", not bad_types, bad_types or "ok", "required columns and types")

    add("min_rows", "FAIL", n >= MIN_ROWS, n, MIN_ROWS)

    rows = (accepted.groupBy("Month").agg(F.count("*").alias("n"), F.avg("ArrDel15").alias("rate")).collect())
    by_month = {int(r["Month"]): (int(r["n"]), float(r["rate"])) for r in rows}
    missing = [m for m in REQUIRED_MONTHS if m not in by_month]
    add("months_present", "FAIL", not missing, {"missing": missing}, REQUIRED_MONTHS)

    overall = float(accepted.agg(F.avg("ArrDel15")).collect()[0][0] or 0.0)
    add("label_rate_overall", "FAIL", OVERALL_RATE[0] <= overall <= OVERALL_RATE[1], round(overall, 4), OVERALL_RATE)

    off = {m: round(r, 4) for m, (_, r) in sorted(by_month.items()) if not (MONTH_RATE[0] <= r <= MONTH_RATE[1])}
    add("label_rate_by_month", "FAIL", not off, {"months_out_of_range": off}, MONTH_RATE)

    dups = n - accepted.select(*KEY).distinct().count()
    add("no_duplicates", "FAIL", dups == 0, dups, 0)

    add("retention", "FAIL", clean["retention"] >= MIN_RETENTION, round(clean["retention"], 5), MIN_RETENTION)

    quar = accepted.where(quarantine_condition()).count()
    share = quar / n if n else 1.0
    add("quarantine_share", "FAIL", share <= MAX_QUARANTINE_SHARE, round(share, 5), MAX_QUARANTINE_SHARE)
    add("bad_scheduled_elapsed", "QUARANTINE", True, quar, "rows removed from gold")

    if by_month:
        big, small = max(v[0] for v in by_month.values()), min(v[0] for v in by_month.values())
        add("month_imbalance", "WARN", small / big >= WARN_IMBALANCE, round(small / big, 3), WARN_IMBALANCE)
    sp = {r["data_split"]: float(r["r"]) for r in accepted.groupBy("data_split").agg(F.avg("ArrDel15").alias("r")).collect()}
    if "train" in sp and "test" in sp:
        add("split_rate_shift", "WARN", abs(sp["train"] - sp["test"]) <= WARN_SPLIT_SHIFT,
            round(abs(sp["train"] - sp["test"]), 4), WARN_SPLIT_SHIFT)

    failed = [r["rule"] for r in results if r["policy"] == "FAIL" and not r["passed"]]
    warnings = [r["rule"] for r in results if r["policy"] == "WARN" and not r["passed"]]
    return results, failed, warnings, n, quar

# COMMAND ----------
# Feature set v1: schedule-only columns, all known two hours before departure. See FEATURES.md.
def make_features(df):
    return df.select(
        F.col("FlightDate").alias("flight_date"),
        F.col("Flight_Number_Operating_Airline").alias("flight_number"),
        F.col("Operating_Airline").alias("carrier"),
        F.col("Origin").alias("origin"),
        F.col("Dest").alias("dest"),
        F.concat_ws("-", F.col("Origin"), F.col("Dest")).alias("route"),
        (F.floor(F.col("CRSDepTime") / 100) % 24).cast("int").alias("dep_hour"),
        (F.floor(F.col("CRSArrTime") / 100) % 24).cast("int").alias("arr_hour"),
        F.col("DayOfWeek").cast("int").alias("day_of_week"),
        F.col("Month").cast("int").alias("month"),
        F.col("Distance").alias("distance"),
        F.col("CRSElapsedTime").alias("sched_elapsed_min"),
        (F.col("DayOfWeek") >= 6).alias("is_weekend"),
        F.col("data_split"),
        F.col("ArrDel15").cast("int").alias("label_arr_del15"),
    )

print("common loaded | run_id:", RUN_ID or "(none)", "| input:", INPUT_DIR)
