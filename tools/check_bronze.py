import duckdb, sys
CSV = "work/source/Combined_Flights_2022.csv"
PQ = "work/bronze/*/*.parquet"
con = duckdb.connect()
raw = f"read_csv('{CSV}', sample_size=-1)"
out = f"read_parquet('{PQ}', filename=true, hive_partitioning=false)"
fail = []

exp = dict(con.execute(f"SELECT Month, count(*) FROM {raw} GROUP BY 1").fetchall())
got = con.execute(
    f"SELECT Month, regexp_extract(filename, 'month=([0-9]+)', 1), count(*) FROM {out} GROUP BY 1,2 ORDER BY 1").fetchall()
print("Month | folder | rows in Parquet | rows in raw CSV")
for m, folder, n in got:
    print(m, folder, n, exp.get(m))
    if int(folder) != m: fail.append(f"month {m} is in folder {folder}")
    if exp.get(m) != n: fail.append(f"month {m}: parquet {n} vs raw {exp.get(m)}")
if set(exp) != {m for m, _, _ in got}: fail.append("months present differ from raw")

tot_raw = sum(exp.values())
tot_pq = con.execute(f"SELECT count(*) FROM {out}").fetchone()[0]
print("total raw:", tot_raw, "total parquet:", tot_pq)
if tot_raw != tot_pq: fail.append("total rows differ")

nulls = lambda src: con.execute(f"SELECT count(*) FROM {src} WHERE ArrDel15 IS NULL").fetchone()[0]
print("ArrDel15 nulls raw/parquet:", nulls(raw), nulls(out))
if nulls(raw) != nulls(out): fail.append("ArrDel15 null counts differ")

types = dict((r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM {out}").fetchall())
for col, want in [("FlightDate", "DATE"), ("Cancelled", "BOOLEAN"), ("ArrDel15", "DOUBLE")]:
    print(col, types.get(col))
    if types.get(col) != want: fail.append(f"{col} is {types.get(col)}, expected {want}")
if "data_split" not in types: fail.append("data_split column missing")

print("FAIL: " + "; ".join(fail) if fail else "PASS: bronze matches raw by month, nulls and types")
sys.exit(1 if fail else 0)
