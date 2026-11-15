#!/usr/bin/env python3
"""Check a FeatureSetSpec.yaml against the Parquet rows it will point at (Lab-7 count_rows.py, adapted).

Registration only checks the YAML. This opens the data: every column the spec names must exist with the right type,
and the row count must be what you expect.

Usage: python3 tools/check_spec.py spec/flight_features_spec_v2/FeatureSetSpec.yaml "work/features_ts/*.parquet" [expected_rows]
"""
import sys, glob
import yaml, duckdb

spec_path, pattern = sys.argv[1], sys.argv[2]
expected = int(sys.argv[3]) if len(sys.argv) > 3 else None

spec = yaml.safe_load(open(spec_path))
files = sorted(glob.glob(pattern))
if not files:
    sys.exit(f"STOP: no Parquet files match {pattern}")

con = duckdb.connect()
cols = {n: t.upper() for n, t, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{pattern}')").fetchall()}
rows = con.execute(f"SELECT count(*) FROM read_parquet('{pattern}')").fetchone()[0]

# spec type -> accepted Parquet types
OK = {"string": {"VARCHAR"}, "long": {"BIGINT"}, "integer": {"INTEGER"}, "double": {"DOUBLE"}, "float": {"FLOAT"},
      "boolean": {"BOOLEAN"}, "datetime": {"TIMESTAMP WITH TIME ZONE", "TIMESTAMP"}}

problems = []
want = [(f["name"], f["type"], "feature") for f in spec["features"]]
want += [(i["name"], i["type"], "index") for i in spec["index_columns"]]
want += [(spec["source"]["timestamp_column"]["name"], "datetime", "timestamp")]

print("spec     ", spec_path)
print("source   ", spec["source"]["path"])
print("resolves ", len(files), "part file(s)")
print("rows     ", f"{rows:,}")
print("columns   in data:", len(cols), "| named by the spec:", len(want),
      f"({sum(1 for w in want if w[2]=='index')} index + 1 timestamp + {sum(1 for w in want if w[2]=='feature')} features)")
for name, typ, kind in want:
    have = cols.get(name)
    good = have is not None and have in OK.get(typ, set())
    print(f"  {'ok  ' if good else 'FAIL'} {kind:<9} {name:<28} spec={typ:<9} data={have}")
    if not good:
        problems.append(f"{name}: spec says {typ}, data has {have}")
if expected is not None and rows != expected:
    problems.append(f"row count {rows} differs from expected {expected}")
if len(set(n for n, _, _ in want)) != len(want):
    problems.append("a column is named twice in the spec")

if problems:
    print("\nPROBLEMS")
    for p in problems:
        print(" -", p)
    sys.exit(1)
print("\nSPEC RESOLVES TO REAL ROWS")
