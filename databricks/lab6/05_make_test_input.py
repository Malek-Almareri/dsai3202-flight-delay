# Databricks notebook source
# MAGIC %md
# MAGIC # 05_make_test_input · run once by hand (not a job task)
# MAGIC Builds a small, separate test input for the controlled quality failure.
# MAGIC
# MAGIC 1. Takes a 2% random sample of every month of the real bronze data (fixed seed).
# MAGIC 2. In **May only**, marks every completed flight as delayed (`ArrDel15 = 1`, delay = 45 minutes). The label stays consistent with the delay
# MAGIC    minutes, so the cleaning rules accept these rows; only the gate rule `label_rate_by_month` should catch them.
# MAGIC 3. Writes it to `processed/flights/combined_flights_2022/<version>/bronze_test/`. The real `bronze/` folder and the raw CSV are never touched.
# MAGIC
# MAGIC Prediction to write down before running the job on this input: `clean` succeeds, `validate` fails on `label_rate_by_month`, `publish` does not run.

# COMMAND ----------
# MAGIC %run ./00_common_flights

# COMMAND ----------
if not LAKE:
    raise Exception("Set the lake widget (and schema) first.")
REAL = lake_path("processed", f"{BASE}/bronze")
TEST = lake_path("processed", f"{BASE}/bronze_test")
SEED = 42

real = spark.read.option("recursiveFileLookup", "true").parquet(REAL)
sample = real.sampleBy("Month", {m: 0.02 for m in range(1, 8)}, seed=SEED)

completed = (F.col("Cancelled") == False) & (F.col("Diverted") == False)
may = F.col("Month") == 5
test = (sample
        .withColumn("ArrDel15", F.when(may & completed, F.lit(1.0)).otherwise(F.col("ArrDel15")))
        .withColumn("ArrDelayMinutes", F.when(may & completed, F.lit(45.0)).otherwise(F.col("ArrDelayMinutes")))
        .withColumn("ArrDelay", F.when(may & completed, F.lit(45.0)).otherwise(F.col("ArrDelay"))))

test.write.mode("overwrite").parquet(TEST)

back = spark.read.option("recursiveFileLookup", "true").parquet(TEST)
per_month = {int(r["Month"]): (int(r["n"]), round(float(r["rate"]), 3)) for r in
             back.where(completed).groupBy("Month").agg(F.count("*").alias("n"), F.avg("ArrDel15").alias("rate")).collect()}
print("test input:", TEST)
print("rows:", back.count())
print("month: (completed flights, delay rate):", dict(sorted(per_month.items())))

# COMMAND ----------
manifest = {
    "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "source": REAL, "test_input": TEST, "sample_fraction_per_month": 0.02, "seed": SEED,
    "injected_defect": "May: every completed flight set to ArrDel15=1 and 45 delay minutes",
    "expected_failing_rule": "label_rate_by_month",
    "rows": back.count(), "month_summary": per_month,
}
path = lake_path("curated", f"{BASE}/quality/test_inputs/bronze_test_manifest.json")
dbutils.fs.put(path, json.dumps(manifest, indent=2, default=str), True)
print("saved", path)
