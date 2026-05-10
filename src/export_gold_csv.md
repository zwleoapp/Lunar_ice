# Export Gold v0.04 → Annotated CSV
<!-- created_by_agent -->

```python
# ── Cell 1: Load + Annotate ───────────────────────────────────────────────────
import pandas as pd

GOLD     = "lunar_ice.south_pole.gold_v4_shackleton_precision"
OUT_PATH = "/Users/gwu/Documents/Lunar_ice/data/gold_v4_psr_rankings.csv"

df = spark.sql(f"""
    SELECT
        cell_lat,
        cell_lon,
        pass_count,
        ROUND(csetn1_avg,    4) AS csetn1_avg,
        ROUND(n_suppression, 4) AS nsi,
        ROUND(slope_avg_deg, 2) AS slope_deg,
        ROUND(rpi_real,      6) AS rpi,
        nearest_psr_name,
        ROUND(distance_km,   1) AS distance_km,
        within_psr
    FROM {GOLD}
    ORDER BY rpi DESC, nsi DESC
""").toPandas()

# ── priority_tier ─────────────────────────────────────────────────────────────
# Prime   : strong ice signal + accessible terrain
# Watch   : moderate suppression — worth monitoring
# Background: insufficient signal or steep terrain
def tier(row):
    if row["nsi"] >= 0.6 and row["slope_deg"] <= 6.0:
        return "Prime"
    elif row["nsi"] >= 0.3:
        return "Watch"
    return "Background"

df["priority_tier"] = df.apply(tier, axis=1)

# ── annotation for notable cells ─────────────────────────────────────────────
def annotate(row):
    psr = row["nearest_psr_name"] or ""
    if row["within_psr"] and row["nsi"] >= 0.5:
        return f"confirmed within {psr} PSR — strong suppression"
    elif row["within_psr"]:
        return f"confirmed within {psr} PSR boundary"
    elif row["nsi"] >= 0.7:
        return f"strong CSETN suppression near {psr}"
    return ""

df["annotation"] = df.apply(annotate, axis=1)

counts = df["priority_tier"].value_counts()
print(f"Total cells : {len(df):,}")
print(f"  Prime     : {counts.get('Prime',      0):,}  (NSI ≥ 0.6, slope ≤ 6°)")
print(f"  Watch     : {counts.get('Watch',      0):,}  (NSI ≥ 0.3)")
print(f"  Background: {counts.get('Background', 0):,}")
print(f"  Within PSR: {int(df['within_psr'].sum()):,}")
print(f"\nTop-5 by RPI:")
print(df[["cell_lat","cell_lon","nsi","slope_deg","rpi","nearest_psr_name","priority_tier","annotation"]].head(5).to_string(index=False))
```

```python
# ── Cell 2: Write CSV to UC Volume ────────────────────────────────────────────
df.to_csv(OUT_PATH, index=False)
n = len(df)
print(f"Written {n:,} rows → {OUT_PATH}")
print(f"Columns: {list(df.columns)}")
print(f"\nData range: lat {df.cell_lat.min():.3f}° to {df.cell_lat.max():.3f}°")
print(f"            lon {df.cell_lon.min():.3f}° to {df.cell_lon.max():.3f}°")
print(f"\nPSR summary:")
print(df.groupby("nearest_psr_name")[["nsi","rpi"]].mean().round(4).to_string())
```
