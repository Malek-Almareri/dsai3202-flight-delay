# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 · Bronze to Silver · US flights 2022
# MAGIC Adapted from the Lab-5 reviews notebook (01_silver.py) for `Combined_Flights_2022.csv`.
# MAGIC
# MAGIC Input: `processed/flights/combined_flights_2022/<version>/bronze/` (written by ADF in Lab 4).
# MAGIC Outputs: silver Delta table, saved rejected rows, and a quality report.
# MAGIC
# MAGIC Run the cells in order. Do not run a second writer at the same time.

# COMMAND ----------

# 2.1 Widgets, names and paths. The first blank run stops normally.
import json, uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window as W

dbutils.widgets.text("lake", "amazonlake60303551")
dbutils.widgets.text("catalog", "dsai3202_databricks")
dbutils.widgets.text("schema", "s_60303551")
dbutils.widgets.text("version", "v1")
dbutils.widgets.text("expected_rows", "4078318")  # raw CSV data rows from tools/flights.py inspect

LAKE = dbutils.widgets.get("lake").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VERSION = dbutils.widgets.get("version").strip()
EXPECTED_ROWS = int(dbutils.widgets.get("expected_rows"))

if not LAKE or not SCHEMA:
    dbutils.notebook.exit("Fill in the lake and schema widgets (schema name only), then run this cell again.")

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
BASE = f"flights/combined_flights_2022/{VERSION}"

def lake_path(container, rel):
    return f"abfss://{container}@{LAKE}.dfs.core.windows.net/{rel}"

BRONZE = lake_path("processed", f"{BASE}/bronze")
SILVER = lake_path("processed", f"{BASE}/silver_flights")
REJECTS = lake_path("processed", f"{BASE}/silver_flights_rejects")
REPORT_DIR = lake_path("curated", f"{BASE}/quality")
TABLE = f"{CATALOG}.{SCHEMA}.silver_flights"

print("run_id :", RUN_ID)
print("bronze :", BRONZE)
print("silver :", SILVER)
print("table  :", TABLE)

# COMMAND ----------

# 2.2 Read the bronze input and check it against the raw CSV counts.
# recursiveFileLookup reads all month=XX folders without turning the folder name into a second 'month' column.
bronze = (spark.read.option("recursiveFileLookup", "true").parquet(BRONZE)
          .select("*", F.col("_metadata.file_path").alias("source_file")))
COLS = [c for c in bronze.columns if c != "source_file"]

n_in = bronze.count()
per_month = {r["Month"]: r["count"] for r in bronze.groupBy("Month").count().collect()}
EXPECTED_MONTHS = {1: 563737, 2: 519952, 3: 590542, 4: 580290, 5: 602950, 6: 602057, 7: 618790}

print("input rows:", n_in, "| raw CSV rows:", EXPECTED_ROWS)
print("per month :", dict(sorted(per_month.items())))
print("columns   :", len(COLS), "| key types:", {c: t for c, t in bronze.dtypes
      if c in ("FlightDate", "Cancelled", "Diverted", "ArrDel15", "Month", "data_split")})

assert n_in == EXPECTED_ROWS, ("Input row count differs from the raw CSV. Do not remove rows to force a match: "
                               "check the ADF output for repeated writes or missing months.")
bad = {m: (per_month.get(m), n) for m, n in EXPECTED_MONTHS.items() if per_month.get(m) != n}
assert not bad, f"Month counts differ from the raw CSV (month: (bronze, raw)): {bad}"
assert "data_split" in COLS, "data_split column missing: check the Lab-4 data flow."
print("OK: input matches the raw CSV, month by month.")

# COMMAND ----------

# 3.0 Probe the duplicate key before cleaning (information only).
KEY = ["FlightDate", "Operating_Airline", "Flight_Number_Operating_Airline", "Origin", "Dest", "CRSDepTime"]
distinct_keys = bronze.select(*KEY).distinct().count()
print("rows:", n_in, "| distinct keys:", distinct_keys, "| rows sharing a key with another row:", n_in - distinct_keys)

# COMMAND ----------

# 3.1 Cleaning rules. Each row gets the FIRST rule it fails, so counts never overlap.
def blank(c):
    return F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")

def bad_hhmm(c):  # scheduled times are HHMM: 0..2400 with minutes under 60
    return F.col(c).isNull() | (F.col(c) < 0) | (F.col(c) > 2400) | ((F.col(c) % 100) >= 60)

RULES = [  # (reason, condition that is TRUE when the row fails), in order
    ("missing_key",
     blank("FlightDate") | blank("Operating_Airline") | blank("Origin") | blank("Dest")
     | blank("Flight_Number_Operating_Airline") | blank("CRSDepTime")
     | F.col("Cancelled").isNull() | F.col("Diverted").isNull()),
    ("excluded_cancelled", F.col("Cancelled")),   # task definition: no arrival outcome
    ("excluded_diverted", F.col("Diverted")),
    ("bad_label", F.col("ArrDel15").isNull() | ~F.col("ArrDel15").isin(0.0, 1.0)),
    ("date_out_of_range",
     (F.col("FlightDate") < F.lit("2022-01-01").cast("date")) | (F.col("FlightDate") > F.lit("2022-07-31").cast("date"))),
    ("month_mismatch", (F.month("FlightDate") != F.col("Month")) | (F.year("FlightDate") != F.col("Year"))),
    ("bad_schedule_time", bad_hhmm("CRSDepTime") | bad_hhmm("CRSArrTime")),
    ("bad_airport",
     (F.length(F.trim(F.col("Origin"))) != 3) | (F.length(F.trim(F.col("Dest"))) != 3) | (F.col("Origin") == F.col("Dest"))),
    ("bad_distance", F.col("Distance").isNull() | (F.col("Distance") <= 0)),
    ("label_inconsistent",
     F.col("ArrDelayMinutes").isNull()
     | ((F.col("ArrDel15") == 1) & (F.col("ArrDelayMinutes") < 15))
     | ((F.col("ArrDel15") == 0) & (F.col("ArrDelayMinutes") >= 15))),
]

reason = None
for name, cond in RULES:
    reason = F.when(cond, name) if reason is None else reason.when(cond, name)

flagged = bronze.withColumn("reject_reason", reason).persist()
valid = flagged.where(F.col("reject_reason").isNull())

# Duplicate rule: one row per KEY. Tie-break: the lexically smallest full payload (reproducible, not proof of truth).
w = W.partitionBy(*KEY).orderBy(F.to_json(F.struct(*[F.col(c) for c in COLS])).asc())
ranked = valid.withColumn("_rn", F.row_number().over(w))
accepted = ranked.where("_rn = 1").drop("_rn", "reject_reason", "source_file")
dup_rows = ranked.where("_rn > 1").drop("_rn").withColumn("reject_reason", F.lit("duplicate_key"))
print("Rules defined:", [n for n, _ in RULES] + ["duplicate_key"])

# COMMAND ----------

# 3.2 Apply the rules, check the balance, and decide whether publication may continue.
rej_counts = {r["reject_reason"]: r["count"] for r in
              flagged.where(F.col("reject_reason").isNotNull()).groupBy("reject_reason").count().collect()}
n_dup = dup_rows.count()
n_acc = accepted.count()
excluded = sum(v for k, v in rej_counts.items() if k.startswith("excluded_"))
invalid = sum(v for k, v in rej_counts.items() if not k.startswith("excluded_"))

print("input rows        :", n_in)
print("excluded          :", {k: v for k, v in rej_counts.items() if k.startswith("excluded_")})
print("invalid           :", {k: v for k, v in rej_counts.items() if not k.startswith("excluded_")})
print("duplicate copies  :", n_dup)
print("accepted          :", n_acc)
balanced = n_in == n_acc + excluded + invalid + n_dup
print(("BALANCED" if balanced else "NOT BALANCED"),
      f": input {n_in} = accepted {n_acc} + excluded {excluded} + invalid {invalid} + duplicate copies {n_dup}")

# Publication gate: stop before anything is written if this fails.
MAX_BAD_RATE = 0.01  # invalid + duplicate rows, as a share of rows that were not excluded by design
considered = n_in - excluded
bad_rate = (invalid + n_dup) / considered if considered else 1.0
gate_ok = balanced and n_acc > 0 and bad_rate <= MAX_BAD_RATE
print(f"quality gate: bad rate {bad_rate:.4%} (limit {MAX_BAD_RATE:.2%}) -> {'PASS' if gate_ok else 'FAIL'}")
if not gate_ok:
    raise Exception("QUALITY GATE FAILED: nothing is published. Read the counts above and fix the cause.")

for r in [k for k in rej_counts if not k.startswith("excluded_")]:
    print("Example rejected rows for rule:", r)
    display(flagged.where(F.col("reject_reason") == r)
            .select("reject_reason", "FlightDate", "Airline", "Origin", "Dest", "CRSDepTime", "Distance", "ArrDel15", "ArrDelayMinutes")
            .limit(3))

# COMMAND ----------

# 4.1 Save rejected, excluded and duplicate rows with their reason, then read them back.
def shape(df):
    return df.select("reject_reason", "source_file", "FlightDate", "Airline", "Origin", "Dest", "Month",
                     F.to_json(F.struct(*[F.col(c) for c in COLS])).alias("_raw_json"))

rejects = (shape(flagged.where(F.col("reject_reason").isNotNull()))
           .unionByName(shape(dup_rows)))
REJ_PATH = f"{REJECTS}/run_id={RUN_ID}"
rejects.write.mode("overwrite").parquet(REJ_PATH)

saved = spark.read.parquet(REJ_PATH).count()
expected_saved = excluded + invalid + n_dup
print("reject path :", REJ_PATH)
print("saved rows  :", saved, "| expected (excluded + invalid + duplicates):", expected_saved)
assert saved == expected_saved, "Saved reject count does not match. Do not continue."

# COMMAND ----------

# 5.1 Write the silver Delta table (full rebuild, partitioned by Month) and register it in the catalog.
from delta.tables import DeltaTable

def write_silver():
    (accepted.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").partitionBy("Month").save(SILVER))

def latest_version():
    return DeltaTable.forPath(spark, SILVER).history(1).collect()[0]["version"]

write_silver()
spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} USING DELTA LOCATION '{SILVER}'")
first_version = latest_version()

detail = spark.sql(f"DESCRIBE DETAIL delta.`{SILVER}`").select("format", "location", "partitionColumns").collect()[0]
print("format:", detail["format"], "| partition columns:", detail["partitionColumns"])
print("location:", detail["location"])
print("first write is Delta version", first_version, "| table:", TABLE)

# COMMAND ----------

# 5.2 Prove the rerun: write again, then compare the two versions in both directions.
write_silver()
repeat_version = latest_version()

a = spark.read.format("delta").option("versionAsOf", first_version).load(SILVER)
b = spark.read.format("delta").option("versionAsOf", repeat_version).load(SILVER)
same = (a.count() == b.count()
        and a.exceptAll(b).limit(1).count() == 0
        and b.exceptAll(a).limit(1).count() == 0)
if not same:
    raise Exception("DIFFERENT CONTENT between the two versions. Do not continue.")
print(f"SAME CONTENT: version {first_version} and version {repeat_version} hold identical rows")
display(DeltaTable.forPath(spark, SILVER).history(5).select("version", "timestamp", "operation"))

# COMMAND ----------

# 7.1 Final read-back and quality report (written last).
silver = spark.read.format("delta").load(SILVER)
rows_out = silver.count()
assert rows_out == n_acc == spark.table(TABLE).count(), "Silver rows differ from accepted rows or from the catalog table."
print("per month in silver:", {r["Month"]: r["count"] for r in silver.groupBy("Month").count().orderBy("Month").collect()})

report = {
    "run_id": RUN_ID,
    "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "input_path": BRONZE,
    "input_rows": n_in,
    "excluded": {k: v for k, v in rej_counts.items() if k.startswith("excluded_")},
    "invalid": {k: v for k, v in rej_counts.items() if not k.startswith("excluded_")},
    "duplicate_copies": n_dup,
    "rows_out": rows_out,
    "balanced": balanced,
    "gate": {"max_bad_rate": MAX_BAD_RATE, "bad_rate": bad_rate, "passed": gate_ok},
    "duplicate_key": KEY,
    "silver_path": SILVER,
    "silver_table": TABLE,
    "silver_version": repeat_version,
    "reject_path": REJ_PATH,
    "rules_in_order": [n for n, _ in RULES] + ["duplicate_key"],
}
dbutils.fs.put(f"{REPORT_DIR}/runs/{RUN_ID}.json", json.dumps(report, indent=2), True)
dbutils.fs.put(f"{REPORT_DIR}/drop_report.json", json.dumps(report, indent=2), True)
print(json.dumps(report, indent=2))