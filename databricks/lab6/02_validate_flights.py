# Databricks notebook source
# MAGIC %md
# MAGIC # 02_validate_flights · job task 2
# MAGIC Checks this run's accepted rows against the gate rules in `00_common_flights` (section "Quality gate rules"):
# MAGIC
# MAGIC | Policy | What happens |
# MAGIC |---|---|
# MAGIC | FAIL | The task raises an error, so `publish` does not run |
# MAGIC | QUARANTINE | The rows are saved for review and left out of gold; the run continues |
# MAGIC | WARN | A note is saved in `gate.json`; the run continues |
# MAGIC
# MAGIC The verdict is saved in `quality/lab6_runs/<run_id>/gate.json` before any error is raised.

# COMMAND ----------
# MAGIC %run ./00_common_flights

# COMMAND ----------
require_run()
if exists(f"{QDIR}/gate.json"):
    raise Exception("gate.json already exists for this run_id. Start a fresh Run now.")

accepted = spark.read.parquet(f"{RUN_ROOT}/accepted")
clean = read_json(f"{QDIR}/clean.json")
print("checking", clean["accepted"], "accepted rows from", clean["input_dir"])

results, failed, warnings, n, quar = evaluate_gate(accepted, clean)
for r in results:
    print(f"{'PASS' if r['passed'] else 'FAILED':<7} {r['policy'].lower() + '-rule':<14} {r['rule']:<24} value={r['value']} limit={r['limit']}")

# COMMAND ----------
# Save quarantined rows for review (only when there are some), then the verdict.
if quar > 0:
    accepted.where(quarantine_condition()).write.mode("error").parquet(f"{RUN_ROOT}/quarantine")

passed = not failed
save_json(f"{QDIR}/gate.json", {
    "run_id": RUN_ID, "input_dir": INPUT_DIR, "passed": passed, "failed_rules": failed, "warnings": warnings,
    "rows_checked": n, "quarantined": quar, "rows_after_quarantine": n - quar,
    "gold_version_seen": latest_gold_version(), "results": results,
})

if not passed:
    raise Exception("QUALITY GATE FAILED: " + ", ".join(failed))
print("QUALITY GATE PASSED | warnings:", warnings or "none")
