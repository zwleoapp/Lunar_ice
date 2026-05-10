# Gold v0.04 — CSETN + LOLA RPI (High-Fidelity Sniper Map)
<!-- created_by_agent -->

```python
# ── Cell 1: Config + C_baseline_csetn ───────────────────────────────────────
import json, math, datetime
from pyspark.sql.types import (StructType, StructField, DoubleType,
                                IntegerType, StringType, BooleanType)
from pyspark.sql import functions as F

CONFIG_TABLE = "lunar_ice.south_pole.config_gold_params"
cfg = json.loads(
    spark.sql(f"SELECT config_json FROM {CONFIG_TABLE} WHERE config_key='gold_v0.04'")
         .collect()[0]["config_json"]
)
SILVER      = cfg["silver_table"]
SILVER_LOLA = cfg["lola_slope_table"]
GOLD        = cfg["gold_table"]
SLOPE_CLAMP = cfg["slope_clamp_deg"]
MIN_PASSES  = cfg["min_pass_count"]
MOON_RADIUS = cfg["moon_radius_km"]
GRID_DEG    = cfg["grid_deg"]
RPI_THRESH  = cfg["rpi_threshold"]
PSRS        = [{"name": p["name"], "lat": p["lat"], "lon": p["lon"],
                "radius_km": p["diameter_km"] / 2} for p in cfg["psrs"]]

# C_baseline computed live from Silver CSETN1 channel
C_BASELINE_CSETN = round(
    spark.sql(f"SELECT AVG(csetn1_total) AS m FROM {SILVER}").collect()[0]["m"], 4
)
print(f"C_baseline_csetn={C_BASELINE_CSETN}  slope_clamp={SLOPE_CLAMP}°  "
      f"min_passes={MIN_PASSES}  grid={GRID_DEG}°  PSRs={len(PSRS)}")
print(f"Silver: {SILVER}  |  LOLA slopes: {SILVER_LOLA}")
```

```python
# ── Cell 2: CSETN Grid Aggregation (0.25°, pass_count ≥ 3) ──────────────────
grid_df = spark.sql(f"""
    SELECT
        FLOOR(latitude  / {GRID_DEG}) * {GRID_DEG} + {GRID_DEG/2}  AS cell_lat,
        FLOOR(longitude / {GRID_DEG}) * {GRID_DEG} + {GRID_DEG/2}  AS cell_lon,
        COUNT(*)                                                      AS pass_count,
        AVG(csetn1_total)                                             AS csetn1_avg,
        STDDEV(csetn1_total)                                          AS csetn1_stddev,
        MIN(csetn1_total)                                             AS csetn1_min
    FROM {SILVER}
    GROUP BY cell_lat, cell_lon
    HAVING COUNT(*) >= {MIN_PASSES}
""")
cell_count = grid_df.count()
ext = grid_df.selectExpr(
    "ROUND(MIN(cell_lat),4) AS lat_min", "ROUND(MAX(cell_lat),4) AS lat_max",
    "ROUND(AVG(pass_count),1) AS avg_passes", "MAX(pass_count) AS max_passes",
).collect()[0]
print(f"CSETN cells (pass_count >= {MIN_PASSES}): {cell_count:,}")
print(f"  Lat: {ext['lat_min']}° to {ext['lat_max']}°  "
      f"avg_passes={ext['avg_passes']}  max_passes={ext['max_passes']}")
```

```python
# ── Cell 3: NSI + RPI with real LOLA slopes ──────────────────────────────────
# NSI: suppression of csetn1 relative to baseline
nsi_df = grid_df.selectExpr(
    "cell_lat", "cell_lon", "pass_count",
    f"ROUND(csetn1_avg,    4) AS csetn1_avg",
    f"ROUND(csetn1_stddev, 4) AS csetn1_stddev",
    f"ROUND(csetn1_min,    4) AS csetn1_min",
    f"ROUND(GREATEST(0.0, ({C_BASELINE_CSETN} - csetn1_avg) / {C_BASELINE_CSETN}), 6) AS n_suppression",
)

# Join LOLA slopes
lola_df = spark.table(SILVER_LOLA).select("cell_lat", "cell_lon", "slope_avg_deg")

rpi_df = nsi_df.join(lola_df, on=["cell_lat", "cell_lon"], how="left") \
    .selectExpr(
        "cell_lat", "cell_lon", "pass_count",
        "csetn1_avg", "csetn1_stddev", "csetn1_min",
        "n_suppression",
        "slope_avg_deg",
        f"ROUND(n_suppression / GREATEST(COALESCE(slope_avg_deg, {SLOPE_CLAMP}), {SLOPE_CLAMP}), 6) AS rpi_real",
        f"COALESCE(slope_avg_deg, {SLOPE_CLAMP}) AS slope_used_deg",
    )

stats = rpi_df.selectExpr(
    "SUM(CASE WHEN n_suppression > 0 THEN 1 ELSE 0 END) AS suppressed",
    "ROUND(MAX(n_suppression),4) AS max_nsi",
    "ROUND(AVG(slope_avg_deg),3) AS avg_slope",
    "ROUND(MIN(slope_avg_deg),3) AS min_slope",
    "ROUND(MAX(rpi_real),4)      AS max_rpi",
    "ROUND(AVG(rpi_real),4)      AS avg_rpi",
    f"SUM(CASE WHEN rpi_real >= {RPI_THRESH} THEN 1 ELSE 0 END) AS prime_cells",
    "SUM(CASE WHEN slope_avg_deg IS NULL THEN 1 ELSE 0 END) AS no_lola",
).collect()[0]

print(f"Cells with positive NSI: {stats['suppressed']:,}")
print(f"  max_NSI={stats['max_nsi']}  avg_slope={stats['avg_slope']}°  min_slope={stats['min_slope']}°")
print(f"  max_RPI={stats['max_rpi']}  avg_RPI={stats['avg_rpi']}")
print(f"  Cells with RPI >= {RPI_THRESH}: {stats['prime_cells']} (Prime Extraction Sites)")
print(f"  Cells with no LOLA slope: {stats['no_lola']} (using clamp={SLOPE_CLAMP}°)")
```

```python
# ── Cell 4: PSR Haversine Cross-Reference ────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2, R=MOON_RADIUS):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

rows        = rpi_df.collect()
enriched    = []
ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

for r in rows:
    best_name = None; best_dist = float("inf"); best_within = False
    for p in PSRS:
        d = haversine_km(r.cell_lat, r.cell_lon, p["lat"], p["lon"])
        if d < best_dist:
            best_dist = d; best_name = p["name"]
            best_within = d <= p["radius_km"]
    enriched.append((
        float(r.cell_lat), float(r.cell_lon), int(r.pass_count),
        float(r.csetn1_avg or 0), float(r.csetn1_stddev or 0), float(r.csetn1_min or 0),
        float(r.n_suppression), float(r.slope_used_deg or SLOPE_CLAMP), float(r.rpi_real),
        best_name, round(best_dist, 3), best_within, ingested_at, "true"
    ))

within_count = sum(1 for row in enriched if row[11])
prime_count  = sum(1 for row in enriched if row[8] >= RPI_THRESH)
print(f"Enriched {len(enriched):,} cells  |  within PSR: {within_count:,}  |  prime (RPI>={RPI_THRESH}): {prime_count}")
```

```python
# ── Cell 5: Write gold_v4_shackleton_precision + Verify ──────────────────────
SCHEMA = StructType([
    StructField("cell_lat",         DoubleType(),  False),
    StructField("cell_lon",         DoubleType(),  False),
    StructField("pass_count",       IntegerType(), False),
    StructField("csetn1_avg",       DoubleType(),  True),
    StructField("csetn1_stddev",    DoubleType(),  True),
    StructField("csetn1_min",       DoubleType(),  True),
    StructField("n_suppression",    DoubleType(),  True),
    StructField("slope_avg_deg",    DoubleType(),  True),
    StructField("rpi_real",         DoubleType(),  True),
    StructField("nearest_psr_name", StringType(),  True),
    StructField("distance_km",      DoubleType(),  True),
    StructField("within_psr",       BooleanType(), True),
    StructField("ingested_at",      StringType(),  True),
    StructField("created_by_agent", StringType(),  True),
])

gold_df = spark.createDataFrame(enriched, schema=SCHEMA)
gold_df.orderBy(F.col("rpi_real").desc()) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD)
spark.sql(f"ALTER TABLE {GOLD} SET TBLPROPERTIES ('created_by_agent' = 'true')")

n = spark.sql(f"SELECT COUNT(*) AS n FROM {GOLD}").collect()[0]["n"]
print(f"Written {n:,} rows → {GOLD}")

# Top-10 by RPI
print("\n=== TOP-10 RPI CELLS (CSETN + LOLA slope) ===")
top10 = spark.sql(f"""
    SELECT cell_lat, cell_lon, pass_count,
           ROUND(csetn1_avg,2) AS c1_avg,
           ROUND(n_suppression,4) AS nsi,
           ROUND(slope_avg_deg,2) AS slope,
           ROUND(rpi_real,4) AS rpi,
           nearest_psr_name,
           ROUND(distance_km,1) AS dist_km,
           within_psr
    FROM {GOLD} ORDER BY rpi_real DESC LIMIT 10
""").collect()
for i, r in enumerate(top10, 1):
    tag = "IN PSR" if r["within_psr"] else "      "
    print(f"{i:2}. ({r['cell_lat']:8.4f}°,{r['cell_lon']:8.4f}°) "
          f"pass={r['pass_count']:2}  c1={r['c1_avg']:5.2f}  "
          f"NSI={r['nsi']:.4f}  slope={r['slope']:.2f}°  RPI={r['rpi']:.4f}  "
          f"{r['nearest_psr_name']:<12} {r['dist_km']:6.1f}km {tag}")

# PSR containment check
check = spark.sql(f"""
    SELECT within_psr, ROUND(AVG(rpi_real),4) AS avg_rpi,
           ROUND(AVG(slope_avg_deg),3) AS avg_slope, COUNT(*) AS cells
    FROM {GOLD} GROUP BY within_psr ORDER BY within_psr DESC
""").collect()
print("\n=== RPI by PSR containment ===")
for r in check:
    label = "within PSR " if r["within_psr"] else "outside PSR"
    print(f"  {label}: avg_rpi={r['avg_rpi']}  avg_slope={r['avg_slope']}°  cells={r['cells']:,}")

within_rpi  = next((r["avg_rpi"] for r in check if r["within_psr"]),  None)
outside_rpi = next((r["avg_rpi"] for r in check if not r["within_psr"]), None)
ok = within_rpi and outside_rpi and within_rpi > outside_rpi
print(f"\nWithin RPI > Outside RPI: {'PASS ✓' if ok else 'CHECK — may reflect steep rim averaging'}")
print(f"\n=== Gold v0.04 COMPLETE — {GOLD} ===")
```
