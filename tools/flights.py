#!/usr/bin/env python3
"""Phase 1 ingestion helper. Adapted from Lab 3 tools/data.py for Combined_Flights_2022.csv."""
import hashlib, json, os, subprocess, sys, datetime
from pathlib import Path
import duckdb

SA = os.environ["SA"]                                   # lake name from lab3.env
SRC = Path("work/source/Combined_Flights_2022.csv")     # local copy of the original
PQ = Path("work/Combined_Flights_2022.parquet")         # repaired/converted copy
EVID = Path("evidence")
BASE = "flights/combined_flights_2022/v1"               # versioned path in the lake
RAW_CSV = f"{BASE}/Combined_Flights_2022.csv"
RAW_MAN = f"{BASE}/_manifest.json"
PROC_PQ = f"{BASE}/Combined_Flights_2022.parquet"
CUR_SUM = f"{BASE}/landing-summary.json"
CSV_SQL = f"read_csv('{SRC}', sample_size=-1)"          # -1 = scan whole file to infer types

def sha256(path):                                       # hash in 8 MB chunks (low memory)
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def az(*args):                                          # run an az command, return parsed JSON
    out = subprocess.run(["az", *args, "--only-show-errors", "-o", "json"],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out) if out.strip() else None

def blob_info(container, name):                         # size + ETag of a stored blob
    j = az("storage", "blob", "show", "--container-name", container, "--name", name,
           "--account-name", SA, "--auth-mode", "login")
    return [container, name, str(j["properties"]["contentLength"]), j["properties"]["etag"]]

def remote_sha(container, name):                        # download a blob, hash it, delete the copy
    tmp = Path("work/verify"); tmp.mkdir(parents=True, exist_ok=True)
    f = tmp / "download.tmp"
    subprocess.run(["az", "storage", "blob", "download", "--container-name", container,
                    "--name", name, "--account-name", SA, "--auth-mode", "login",
                    "--file", str(f), "--only-show-errors", "-o", "none"], check=True)
    h = sha256(f); f.unlink()
    return h

def receipts_text():                                    # the "receipt" = name, bytes, ETag per raw file
    rows = [blob_info("raw", RAW_CSV), blob_info("raw", RAW_MAN)]
    return "".join("\t".join(r) + "\n" for r in rows)

def cmd_manifest(laptop_hash):                          # source receipt with identity + hash
    h = sha256(SRC)
    if h != laptop_hash.lower():
        sys.exit(f"HASH MISMATCH: laptop {laptop_hash} vs terminal {h}")
    with open(SRC, "rb") as f:
        lines = sum(1 for _ in f)
    manifest = {
        "dataset": "Flight Status Prediction (Combined_Flights_2022.csv)",
        "source_url": "https://www.kaggle.com/datasets/robikscube/flight-delay-dataset20182022",
        "kaggle_version": "FILL_IN_FROM_KAGGLE_PAGE",
        "file": SRC.name, "size_bytes": SRC.stat().st_size, "sha256": h,
        "csv_lines_including_header": lines,
        "ingestion_mode": "batch, manually triggered",
        "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    }
    EVID.mkdir(exist_ok=True)
    (EVID / "source-manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))

def cmd_inspect():                                      # full-file schema + target sanity check
    con = duckdb.connect()
    cols = con.execute(f"DESCRIBE SELECT * FROM {CSV_SQL}").fetchall()
    print(f"{len(cols)} columns")
    with open(EVID / "schema.tsv", "w") as out:
        for name, typ, *_ in cols:
            print(f"{name}\t{typ}"); out.write(f"{name}\t{typ}\n")
    n = con.execute(f"SELECT count(*) FROM {CSV_SQL}").fetchone()[0]
    print("data rows:", n)
    names = [c[0] for c in cols]
    for need in ["ArrDel15", "Cancelled", "Diverted", "FlightDate"]:
        print(f"column {need}:", "present" if need in names else "MISSING - check the name")
    if "ArrDel15" in names:
        print(con.execute(f"SELECT ArrDel15, count(*) AS n FROM {CSV_SQL} GROUP BY 1 ORDER BY 1").fetchall())

def cmd_verify():                                       # content check: source <-> raw
    m = json.loads((EVID / "source-manifest.json").read_text())
    h = remote_sha("raw", RAW_CSV)
    ok = h == m["sha256"]
    print("MATCH source <-> raw" if ok else f"MISMATCH raw={h} source={m['sha256']}")
    if not ok: sys.exit(1)
    info = blob_info("raw", RAW_CSV)
    (EVID / "raw-baseline.json").write_text(json.dumps(
        {"blob": info, "sha256": h}, indent=2))
    (EVID / "receipts-before.tsv").write_text(receipts_text())
    print("saved raw-baseline.json and receipts-before.tsv")

def cmd_convert():                                      # raw CSV -> Parquet copy, raw untouched
    con = duckdb.connect()
    con.execute("SET memory_limit='1GB'")
    con.execute("SET temp_directory='work/duck_tmp'")
    n_in = con.execute(f"SELECT count(*) FROM {CSV_SQL}").fetchone()[0]
    con.execute(f"COPY (SELECT * FROM {CSV_SQL}) TO '{PQ}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    n_out = con.execute(f"SELECT count(*) FROM read_parquet('{PQ}')").fetchone()[0]
    summary = {"input_rows": n_in, "converted": n_out, "quarantined": n_in - n_out,
               "parquet_bytes": PQ.stat().st_size, "parquet_sha256": sha256(PQ),
               "note": "format conversion only; no cleaning at this stage"}
    Path("work/landing-summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    if n_in != n_out: sys.exit("row accounting failed: input != converted + quarantined")

def cmd_publish_check():                                # local hash == hash of what is in Azure
    items = [("processed", PROC_PQ, PQ), ("curated", CUR_SUM, Path("work/landing-summary.json"))]
    bad = 0
    for container, name, local in items:
        same = remote_sha(container, name) == sha256(local)
        print(("MATCH " if same else "MISMATCH ") + f"{container}/{name}")
        bad += not same
    sys.exit(1 if bad else 0)

def cmd_receipts():                                     # compare receipts before and after a retry
    after = receipts_text()
    (EVID / "receipts-after.tsv").write_text(after)
    before = (EVID / "receipts-before.tsv").read_text()
    print("MATCH receipts unchanged" if before == after else "DIFFERENT - keep both files, investigate")
    sys.exit(0 if before == after else 1)

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"manifest": lambda: cmd_manifest(sys.argv[2]), "inspect": cmd_inspect,
     "verify": cmd_verify, "convert": cmd_convert,
     "publish-check": cmd_publish_check, "receipts": cmd_receipts}.get(
        cmd, lambda: sys.exit("usage: flights.py manifest HASH|inspect|verify|convert|publish-check|receipts"))()
