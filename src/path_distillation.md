---
agent-notebook:
  language: python
---

# Task 1 — Path Distillation: RSCI Unique Paths → config/rsci_paths.csv
<!-- created_by_agent -->

```python
import csv, pathlib

TABLE = "lunar_ice.south_pole.bronze_lend_metadata"

df = spark.sql(f"""
    SELECT DISTINCT path_name, file_name
    FROM {TABLE}
    WHERE product_type = 'LEND_RDR_RSCI'
    ORDER BY path_name, file_name
""")

rows = df.collect()
print(f"Unique RSCI paths: {len(rows)}")
```

```python
out = pathlib.Path("/Users/gwu/Documents/Lunar_ice/config/rsci_paths.csv")
with out.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["path_name", "file_name"])
    for r in rows:
        w.writerow([r["path_name"], r["file_name"]])

print(f"Written: {out}")
print(f"Row count in CSV: {len(rows)}")

# Sanity-check first 3
with out.open() as f:
    for i, line in enumerate(f):
        print(line.rstrip())
        if i >= 3:
            break
```
