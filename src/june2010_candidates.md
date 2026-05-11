---
agent-notebook:
  language: python
---

# Task 1 — Phase + Time Filter: 3-Day Pilot Candidate List
<!-- created_by_agent -->

```python
import csv, pathlib

TABLE = "lunar_ice.south_pole.bronze_lend_metadata"

df = spark.sql(f"""
    SELECT path_name, file_name, start_dt, stop_dt, mission_phase
    FROM {TABLE}
    WHERE product_type = 'LEND_RDR_RSCI'
      AND mission_phase NOT IN ('CRUISE', 'COMMISSIONING')
      AND start_dt >= '2010-06-01'
      AND start_dt <  '2010-06-04'
    ORDER BY start_dt
""")

rows = df.collect()
phases = {r["mission_phase"] for r in rows}
print(f"Candidate rows : {len(rows)}")
print(f"Mission phases : {phases}")
```

```python
out = pathlib.Path("/Users/gwu/Documents/Lunar_ice/config/june2010_3day_candidates.csv")
with out.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["path_name", "file_name", "start_dt", "stop_dt", "mission_phase"])
    for r in rows:
        w.writerow([r["path_name"], r["file_name"], r["start_dt"], r["stop_dt"], r["mission_phase"]])

print(f"Written: {out}")
print()
with out.open() as f:
    for line in f:
        print(line.rstrip())
```
