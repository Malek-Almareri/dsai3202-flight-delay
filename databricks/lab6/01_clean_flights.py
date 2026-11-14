# Databricks notebook source
# MAGIC %md
# MAGIC # 01_clean_flights · job task 1
# MAGIC Reads this run's input (`bronze` or `bronze_test`), applies the cleaning rules from `00_common_flights`, and saves
# MAGIC accepted rows and rejected rows in this run's own folders. It does not decide whether the data is good enough: `02_validate_flights` does.
# MAGIC
# MAGIC | Saved | Where |
# MAGIC |---|---|
# MAGIC | Accepted rows | `curated/flights/.../runs/<run_id>/accepted/` |
# MAGIC | Rejected, excluded and duplicate rows with a reason | `curated/flights/.../runs/<run_id>/rejects/` |
# MAGIC | Counts | `curated/flights/.../quality/lab6_runs/<run_id>/clean.json` |

# COMMAND ----------
# MAGIC %run ./00_common_flights

# COMMAND ----------
require_run()
if exists(QDIR) or exists(RUN_ROOT):
    raise Exception("A saved folder for this run_id already exists. Start a fresh Run now; do not use Repair run.")
START = time.time()
STARTED_UTC = datetime.now(timezone.utc).isoformat(timespec="seconds")

bronze = (spark.read.option("recursiveFileLookup", "true").parquet(INPUT)
          .select("*", F.col("_metadata.file_path").alias("source_file")))
COLS = [c for c in bronze.columns if c != "source_file"]
n_in = bronze.count()
print("input:", INPUT, "| rows:", n_in)

if INPUT_DIR == "bronze" and EXPECTED_ROWS:
    if n_in != int(EXPECTED_ROWS):
        raise Exception(f"Input has {n_in} rows but the raw CSV has {EXPECTED_ROWS}. Check the ADF output for repeated writes or missing months.")
    print("OK: input row count equals the raw CSV row count")

# COMMAND ----------
flagged = flag(bronze).persist()
valid = flagged.where(F.col("reject_reason").isNull())
accepted, dup_rows = dedupe(valid, COLS)

rej_counts = {r["reject_reason"]: r["count"] for r in
              flagged.where(F.col("reject_reason").isNotNull()).groupBy("reject_reason").count().collect()}
n_dup = dup_rows.count()
n_acc = accepted.count()
excluded = sum(v for k, v in rej_counts.items() if k.startswith("excluded_"))
invalid = sum(v for k, v in rej_counts.items() if not k.startswith("excluded_"))
considered = n_in - excluded
retention = 1.0 - ((invalid + n_dup) / considered) if considered else 0.0

print("excluded        :", {k: v for k, v in rej_counts.items() if k.startswith("excluded_")})
print("invalid         :", {k: v for k, v in rej_counts.items() if not k.startswith("excluded_")})
print("duplicate copies:", n_dup)
print("accepted        :", n_acc)
balanced = n_in == n_acc + excluded + invalid + n_dup
print(("BALANCED" if balanced else "NOT BALANCED"),
      f": input {n_in} = accepted {n_acc} + excluded {excluded} + invalid {invalid} + duplicate copies {n_dup}")
if not balanced:
    raise Exception("Row accounting does not balance. Nothing is saved.")

# COMMAND ----------
def shape(df):
    return df.select("reject_reason", "source_file", "FlightDate", "Airline", "Origin", "Dest", "Month",
                     F.to_json(F.struct(*[F.col(c) for c in COLS])).alias("_raw_json"))

rejects = shape(flagged.where(F.col("reject_reason").isNotNull())).unionByName(shape(dup_rows))
accepted.write.mode("error").parquet(f"{RUN_ROOT}/accepted")
rejects.write.mode("error").parquet(f"{RUN_ROOT}/rejects")

saved_acc = spark.read.parquet(f"{RUN_ROOT}/accepted").count()
saved_rej = spark.read.parquet(f"{RUN_ROOT}/rejects").count()
print("saved accepted:", saved_acc, "| saved rejects:", saved_rej)
if saved_acc != n_acc or saved_rej != excluded + invalid + n_dup:
    raise Exception("Saved counts differ from computed counts.")

save_json(f"{QDIR}/clean.json", {
    "run_id": RUN_ID, "input_dir": INPUT_DIR, "input_path": INPUT, "started_utc": STARTED_UTC,
    "start_epoch": START, "input_rows": n_in, "excluded": {k: v for k, v in rej_counts.items() if k.startswith("excluded_")},
    "invalid": {k: v for k, v in rej_counts.items() if not k.startswith("excluded_")},
    "excluded_total": excluded, "invalid_total": invalid, "duplicate_copies": n_dup, "accepted": n_acc,
    "retention": retention, "balanced": balanced, "duplicate_key": KEY,
    "accepted_path": f"{RUN_ROOT}/accepted", "rejects_path": f"{RUN_ROOT}/rejects",
    "rules_in_order": [n for n, _ in RULES] + ["duplicate_key"],
})
print("CLEAN DONE | retention", round(retention, 5))
