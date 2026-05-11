---
agent-notebook:
  language: python
---

# Gold Pre-Flight + Task 1 — Pass Distribution & C_baseline
<!-- created_by_agent -->

```python
SILVER = "lunar_ice.south_pole.silver_lend_targets"
```

```python
# Pre-Flight: pass_count distribution per 0.25° grid cell
dist = spark.sql(f"""
    SELECT pass_count_bucket, COUNT(*) AS cells
    FROM (
        SELECT FLOOR(latitude  / 0.25) * 10000 + FLOOR(longitude / 0.25) AS cell_id,
               COUNT(*) AS pass_count_bucket
        FROM {SILVER}
        GROUP BY cell_id
    )
    GROUP BY pass_count_bucket
    ORDER BY pass_count_bucket
""").collect()

total_cells     = sum(r["cells"] for r in dist)
surviving_cells = sum(r["cells"] for r in dist if r["pass_count_bucket"] >= 3)

print("=== PRE-FLIGHT: Pass-count distribution ===")
print(f"{'pass_count':>12}  {'cells':>8}  {'cumulative %':>14}")
print("-" * 40)
cum = 0
for r in dist:
    cum += r["cells"]
    marker = "  ← gate" if r["pass_count_bucket"] == 3 else ""
    print(f"{r['pass_count_bucket']:>12}  {r['cells']:>8}  {100*cum/total_cells:>13.1f}%{marker}")

print()
print(f"Total distinct cells  : {total_cells:,}")
print(f"Cells with pass >= 3  : {surviving_cells:,}  ({100*surviving_cells/total_cells:.1f}%)")
print(f"Cells dropped (<3)    : {total_cells - surviving_cells:,}")
gate_ok = surviving_cells >= 50
print(f"Gate (>=50 survivors) : {'PASS ✓' if gate_ok else 'FAIL — revisit threshold'}")
```

```python
# Task 1: C_baseline — mean SETN across all Silver rows
baseline_row = spark.sql(f"""
    SELECT
        ROUND(AVG(setn_total),    4)  AS setn_mean,
        ROUND(STDDEV(setn_total), 4)  AS setn_stddev,
        COUNT(*)                      AS row_count
    FROM {SILVER}
""").collect()[0]

c_baseline = baseline_row["setn_mean"]
c_stddev   = baseline_row["setn_stddev"]
n_rows     = baseline_row["row_count"]

print("=== TASK 1: C_baseline ===")
print(f"setn_mean   (C_baseline) : {c_baseline}")
print(f"setn_stddev              : {c_stddev}")
print(f"row_count                : {n_rows:,}")
print()
print(f"Expected: mean ≈ 12.99, stddev ≈ 4.12, count = 71,327")
match_mean = abs(c_baseline - 12.99) < 0.05
match_n    = n_rows == 71327
print(f"Mean match  : {'OK' if match_mean else 'CHECK — unexpected value'}")
print(f"Count match : {'OK' if match_n    else 'CHECK — row count differs'}")
print()
print("=== STOP — awaiting user confirmation before Task 2 ===")
```
