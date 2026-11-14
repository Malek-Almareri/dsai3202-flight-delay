# Databricks notebook source
# MAGIC %md
# MAGIC # 03_publish_flights · job task 3
# MAGIC Publishes only if this run's saved `gate.json` says `passed = true`.
# MAGIC
# MAGIC | Output | Where | Format |
# MAGIC |---|---|---|
# MAGIC | Gold flights | `curated/flights/.../gold_flights` and table `gold_flights` | Delta, partitioned by Month (rebuilt each successful run) |
# MAGIC | Features v1 | `curated/flights/.../runs/<run_id>/features_v1` | Parquet, one folder per run |
# MAGIC | Evidence | `quality/lab6_runs/<run_id>/success.json` | JSON |

# COMMAND ----------
# MAGIC %run ./00_common_flights

# COMMAND ----------
require_run()
gate_path = f"{QDIR}/gate.json"
if not exists(gate_path):
    raise Exception("No gate.json for this run. Publication refused.")
gate = read_json(gate_path)
if not gate.get("passed"):
    raise Exception("The saved gate did not pass. Publication refused.")
if exists(f"{QDIR}/success.json") or exists(f"{RUN_ROOT}/features_v1"):
    raise Exception("This run already published. Start a fresh Run now.")

clean = read_json(f"{QDIR}/clean.json")
accepted = spark.read.parquet(f"{RUN_ROOT}/accepted")
valid = accepted.where(~quarantine_condition())
n_valid = valid.count()
if n_valid != gate["rows_after_quarantine"]:
    raise Exception(f"Rows to publish ({n_valid}) differ from the gate's count ({gate['rows_after_quarantine']}).")

# COMMAND ----------
# Gold: validated flights (all original columns + data_split), full rebuild.
(valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .partitionBy("Month").save(GOLD))
spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} USING DELTA LOCATION '{GOLD}'")
gold_version = latest_gold_version()
gold_rows = spark.read.format("delta").load(GOLD).count()

# Features v1: schedule-only columns, one saved folder per run.
FEATURES = f"{RUN_ROOT}/features_v1"
make_features(valid).write.mode("error").parquet(FEATURES)
feature_rows = spark.read.parquet(FEATURES).count()

if gold_rows != n_valid or feature_rows != n_valid:
    raise Exception("Published row counts differ from the validated rows.")

# COMMAND ----------
balanced = clean["input_rows"] == (clean["excluded_total"] + clean["invalid_total"]
                                   + clean["duplicate_copies"] + gate["quarantined"] + gold_rows)
print(("BALANCED" if balanced else "NOT BALANCED"),
      f": input {clean['input_rows']} = excluded {clean['excluded_total']} + invalid {clean['invalid_total']}"
      f" + duplicate copies {clean['duplicate_copies']} + quarantined {gate['quarantined']} + gold {gold_rows}")
if not balanced:
    raise Exception("Row accounting does not balance after publication.")

save_json(f"{QDIR}/success.json", {
    "run_id": RUN_ID, "input_dir": INPUT_DIR, "gold_path": GOLD, "gold_table": TABLE, "gold_version": gold_version,
    "gold_rows": gold_rows, "features_path": FEATURES, "feature_rows": feature_rows,
    "feature_columns": make_features(valid).columns,
    "input_rows": clean["input_rows"], "excluded_total": clean["excluded_total"], "invalid_total": clean["invalid_total"],
    "duplicate_copies": clean["duplicate_copies"], "quarantined": gate["quarantined"], "warnings": gate["warnings"],
    "pipeline_elapsed_seconds": round(time.time() - clean["start_epoch"], 1),
})
print(f"PUBLISHED | gold version {gold_version} | {gold_rows} rows | features at {FEATURES}")
