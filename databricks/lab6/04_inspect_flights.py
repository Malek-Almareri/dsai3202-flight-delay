# Databricks notebook source
# MAGIC %md
# MAGIC # 04_inspect_flights · run by hand, not a job task
# MAGIC Two jobs in one notebook, kept apart on purpose:
# MAGIC * **Profile (section 2)** describes the published data. It decides nothing.
# MAGIC * **Evidence (sections 3 to 6)** reads the saved results of job runs: counts, the gate verdict, the rerun comparison and the blocked-failure check.
# MAGIC
# MAGIC The widgets `lake` and `schema` at the top must be filled in. Where a line says YOUR VALUE, type the run ID from the job's Runs page.
# MAGIC The profile reads the **whole** published table: no sample is used for exploration.

# COMMAND ----------
# MAGIC %run ./00_common_flights

# COMMAND ----------
# 1.1 READY check
if not LAKE or not SCHEMA:
    raise Exception("Fill in the lake and schema widgets at the top of the notebook, then run this cell again.")
gold = spark.table(TABLE)
gold_rows = gold.count()
print("READY: table", TABLE, "| rows:", gold_rows, "| delta version:", latest_gold_version())

# COMMAND ----------
# 2.1 Population: raw bronze vs gold, month by month and by split
REAL_BRONZE = lake_path("processed", f"{BASE}/bronze")
bronze = spark.read.option("recursiveFileLookup", "true").parquet(REAL_BRONZE)
b = bronze.groupBy("Month").count().withColumnRenamed("count", "bronze_rows")
g = gold.groupBy("Month").count().withColumnRenamed("count", "gold_rows")
pop = (b.join(g, "Month", "left").withColumn("kept_share", F.round(F.col("gold_rows") / F.col("bronze_rows"), 4))
       .orderBy("Month"))
display(pop)
print("bronze rows:", bronze.count(), "| gold rows:", gold_rows)
display(gold.groupBy("data_split").agg(F.count("*").alias("flights"), F.round(F.avg("ArrDel15"), 4).alias("delay_rate")))

# COMMAND ----------
# 2.2 Target balance and the main groups (delay_rate = share of flights arriving 15+ minutes late)
def rate_by(name, expr=None, order=None, limit=None):
    d = gold if expr is None else gold.withColumn(name, expr)
    df = (d.groupBy(name).agg(F.count("*").alias("flights"), F.round(F.avg("ArrDel15"), 4).alias("delay_rate"))
          .orderBy(order if order is not None else name))
    return df.limit(limit) if limit else df

print("overall delay rate:", round(gold.agg(F.avg("ArrDel15")).collect()[0][0], 4))
display(rate_by("Month"))
display(rate_by("Operating_Airline", order=F.desc("flights"), limit=15))
display(rate_by("dep_hour", expr=F.floor(F.col("CRSDepTime") / 100) % 24))
display(rate_by("DayOfWeek"))
display(rate_by("distance_band", expr=F.when(F.col("Distance") < 250, "1: <250")
                 .when(F.col("Distance") < 500, "2: 250-499")
                 .when(F.col("Distance") < 1000, "3: 500-999")
                 .when(F.col("Distance") < 2000, "4: 1000-1999").otherwise("5: 2000+")))

# COMMAND ----------
# 2.3 Correlations with the target. The second group is NOT known before departure: it shows why those columns must stay out of the model.
safe = ["CRSDepTime", "Distance", "CRSElapsedTime", "DayOfWeek", "Month"]
leaky = ["DepDelay", "DepDelayMinutes", "DepDel15", "TaxiOut"]
row = gold.agg(*[F.corr(c, "ArrDel15").alias(c) for c in safe + leaky]).collect()[0].asDict()
print("Known before departure (corr with ArrDel15):")
for c in safe: print(f"  {c:<16} {row[c]:.3f}")
print("Known only after departure (leakage risk):")
for c in leaky: print(f"  {c:<16} {row[c]:.3f}")

# COMMAND ----------
# 2.4 Outliers
for c in ["ArrDelayMinutes", "Distance", "CRSElapsedTime"]:
    q = gold.approxQuantile(c, [0.0, 0.5, 0.9, 0.99, 0.999, 1.0], 0.001)
    print(f"{c:<16} min / median / p90 / p99 / p99.9 / max =", [round(x, 1) for x in q])

# COMMAND ----------
# 2.5 Who is lost? Share of each carrier's flights that reach gold (cancelled, diverted and rejected flights drop out)
bc = bronze.groupBy("Airline").count().withColumnRenamed("count", "bronze_rows")
gc = gold.groupBy("Airline").count().withColumnRenamed("count", "gold_rows")
ret = (bc.join(gc, "Airline", "left").na.fill(0, ["gold_rows"])
       .withColumn("kept_share", F.round(F.col("gold_rows") / F.col("bronze_rows"), 4)).orderBy("kept_share"))
display(ret)

# COMMAND ----------
# 3.1 Evidence for one job run. YOUR VALUE: the Job run ID from the run's details panel.
RUN_ID = "PASTE_RUN_ID"          # YOUR VALUE
P = run_paths(RUN_ID)
clean = read_json(f"{P['q']}/clean.json")
gate = read_json(f"{P['q']}/gate.json") if exists(f"{P['q']}/gate.json") else None
success = read_json(f"{P['q']}/success.json") if exists(f"{P['q']}/success.json") else None
print("run", RUN_ID, "| input:", clean["input_dir"], "| input rows:", clean["input_rows"])
print("gate:", "not reached" if gate is None else ("PASSED" if gate["passed"] else f"FAILED {gate['failed_rules']}"))
print("success.json:", "present" if success else "absent")
if gate:
    for r in gate["results"]:
        print(f"  {'PASS' if r['passed'] else 'FAILED':<7} {r['policy'].lower() + '-rule':<14} {r['rule']:<24} {r['value']}")

# COMMAND ----------
# 4.1 Row accounting for this run
if success:
    total = (clean["excluded_total"] + clean["invalid_total"] + clean["duplicate_copies"]
             + success["quarantined"] + success["gold_rows"])
    print("input", clean["input_rows"], "= excluded", clean["excluded_total"], "+ invalid", clean["invalid_total"],
          "+ duplicate copies", clean["duplicate_copies"], "+ quarantined", success["quarantined"], "+ gold", success["gold_rows"])
    print("BALANCED" if total == clean["input_rows"] else "NOT BALANCED")
    print("elapsed seconds:", success["pipeline_elapsed_seconds"], "| gold version:", success["gold_version"])
else:
    print("This run did not publish, so there is no gold balance to check.")

# COMMAND ----------
# 5.1 Safe rerun: compare this run with a baseline run, row by row. YOUR VALUE: the first successful run ID.
COMPARE_TO = "PASTE_BASELINE_RUN_ID"          # YOUR VALUE
base = read_json(f"{run_paths(COMPARE_TO)['q']}/success.json")
cur = success
assert cur, "RUN_ID above must be a successful run."

def same(a, b):
    return a.count() == b.count() and a.exceptAll(b).limit(1).count() == 0 and b.exceptAll(a).limit(1).count() == 0

gold_a = spark.read.format("delta").option("versionAsOf", base["gold_version"]).load(GOLD)
gold_b = spark.read.format("delta").option("versionAsOf", cur["gold_version"]).load(GOLD)
feat_a = spark.read.parquet(base["features_path"])
feat_b = spark.read.parquet(cur["features_path"])
g_ok, f_ok = same(gold_a, gold_b), same(feat_a, feat_b)
print("run ids differ:", COMPARE_TO != RUN_ID, "| gold versions:", base["gold_version"], "and", cur["gold_version"])
print("gold rows:", base["gold_rows"], "and", cur["gold_rows"], "| feature rows:", base["feature_rows"], "and", cur["feature_rows"])
print("SAME CONTENT (gold)" if g_ok else "DIFFERENT CONTENT (gold)")
print("SAME CONTENT (features)" if f_ok else "DIFFERENT CONTENT (features)")

# COMMAND ----------
# 6.1 Blocked failure: set RUN_ID in cell 3.1 to the FAILED run, then run 3.1 and this cell.
assert gate is not None and not gate["passed"], "RUN_ID must be a run whose gate failed."
checks = {
    "gate failed on a named rule": bool(gate["failed_rules"]),
    "no success.json": success is None,
    "no features folder for this run": not exists(f"{P['root']}/features_v1"),
    "gold table version unchanged": latest_gold_version() == gate["gold_version_seen"],
}
for k, v in checks.items():
    print(("ok   " if v else "FAIL ") + k)
print("failed rules:", gate["failed_rules"])
print("BLOCKED SAFELY" if all(checks.values()) else "NOT BLOCKED: investigate")
