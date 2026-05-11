---
agent-notebook:
  language: python
---

# Gold — RPI Mapping v0.03 (Grid Aggregation, NSI, RPI, PSR Cross-Reference)
<!-- created_by_agent -->

```python
import json, math, datetime
from pyspark.sql.types import (StructType, StructField, DoubleType,
                                IntegerType, StringType, BooleanType)
from pyspark.sql import functions as F

CONFIG_TABLE = "lunar_ice.south_pole.config_gold_params"

cfg = json.loads(
    spark.sql(f"SELECT config_json FROM {CONFIG_TABLE} WHERE config_key='gold_v0.03'")
         .collect()[0]["config_json"]
)

SILVER       = cfg["silver_table"]
GOLD         = cfg["gold_table"]
MOCK_SLOPE   = cfg["mock_slope_deg"]
SLOPE_CLAMP  = cfg["slope_clamp_deg"]
MIN_PASSES   = cfg["min_pass_count"]
MOON_RADIUS  = cfg["moon_radius_km"]
GRID_DEG     = cfg["grid_deg"]
PSRS         = [{"name": p["name"], "lat": p["lat"], "lon": p["lon"],
                  "radius_km": p["diameter_km"] / 2} for p in cfg["psrs"]]

# C_baseline computed live from Silver — never hardcoded
C_BASELINE = round(
    spark.sql(f"SELECT AVG(setn_total) AS m FROM {SILVER}").collect()[0]["m"], 4
)
print(f"C_baseline={C_BASELINE}  mock_slope={MOCK_SLOPE}°  "
      f"min_passes={MIN_PASSES}  grid={GRID_DEG}°  PSRs={len(PSRS)}")
```

```python
# Task 2: FLOOR grid binning + aggregation
grid_df = spark.sql(f"""
    SELECT
        FLOOR(latitude  / {GRID_DEG}) * {GRID_DEG} + {GRID_DEG/2}  AS cell_lat,
        FLOOR(longitude / {GRID_DEG}) * {GRID_DEG} + {GRID_DEG/2}  AS cell_lon,
        COUNT(*)                                                      AS pass_count,
        AVG(setn_total)                                               AS setn_avg,
        STDDEV(setn_total)                                            AS setn_stddev,
        MIN(setn_total)                                               AS setn_min,
        AVG(csetn1_total)                                             AS csetn1_avg
    FROM {SILVER}
    GROUP BY cell_lat, cell_lon
    HAVING COUNT(*) >= {MIN_PASSES}
""")
cell_count = grid_df.count()
ext = grid_df.selectExpr(
    "ROUND(MIN(cell_lat),4) AS lat_min", "ROUND(MAX(cell_lat),4) AS lat_max",
    "ROUND(MIN(cell_lon),4) AS lon_min", "ROUND(MAX(cell_lon),4) AS lon_max",
).collect()[0]
print(f"Surviving cells (pass_count >= {MIN_PASSES}): {cell_count:,}")
print(f"Lat: {ext['lat_min']}° to {ext['lat_max']}°  "
      f"Lon: {ext['lon_min']}° to {ext['lon_max']}°")
```

```python
# Task 3+4: NSI + RPI
nsi_rpi_df = grid_df.selectExpr(
    "cell_lat", "cell_lon", "pass_count",
    "ROUND(setn_avg,    4) AS setn_avg",
    "ROUND(setn_stddev, 4) AS setn_stddev",
    "ROUND(setn_min,    4) AS setn_min",
    "ROUND(csetn1_avg,  4) AS csetn1_avg",
    f"ROUND(GREATEST(0.0, ({C_BASELINE} - setn_avg) / {C_BASELINE}), 6)"
    f"  AS n_suppression",
    f"ROUND(GREATEST(0.0, ({C_BASELINE} - setn_avg) / {C_BASELINE})"
    f"  / GREATEST({MOCK_SLOPE}, {SLOPE_CLAMP}), 6) AS rpi",
)
stats = nsi_rpi_df.selectExpr(
    "SUM(CASE WHEN n_suppression > 0 THEN 1 ELSE 0 END) AS suppressed",
    "ROUND(MAX(n_suppression),4) AS max_nsi",
    "ROUND(MAX(rpi),4)           AS max_rpi",
    "ROUND(AVG(rpi),4)           AS avg_rpi",
).collect()[0]
print(f"Cells with positive NSI: {stats['suppressed']:,}  "
      f"max_NSI={stats['max_nsi']}  max_RPI={stats['max_rpi']}  avg_RPI={stats['avg_rpi']}")
```

```python
# Task 4: PSR Haversine cross-reference
def haversine_km(lat1, lon1, lat2, lon2, R=MOON_RADIUS):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

rows        = nsi_rpi_df.collect()
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
        float(r.setn_avg or 0), float(r.setn_stddev or 0), float(r.setn_min or 0),
        float(r.csetn1_avg or 0), float(r.n_suppression), float(r.rpi),
        best_name, round(best_dist, 3), best_within, ingested_at, "true"
    ))

within_count = sum(1 for row in enriched if row[11])
print(f"Enriched {len(enriched):,} cells  |  within a PSR: {within_count:,}")
```

```python
# Task 5: Write Gold table
SCHEMA = StructType([
    StructField("cell_lat",         DoubleType(),  False),
    StructField("cell_lon",         DoubleType(),  False),
    StructField("pass_count",       IntegerType(), False),
    StructField("setn_avg",         DoubleType(),  True),
    StructField("setn_stddev",      DoubleType(),  True),
    StructField("setn_min",         DoubleType(),  True),
    StructField("csetn1_avg",       DoubleType(),  True),
    StructField("n_suppression",    DoubleType(),  True),
    StructField("rpi",              DoubleType(),  True),
    StructField("nearest_psr_name", StringType(),  True),
    StructField("distance_km",      DoubleType(),  True),
    StructField("within_psr",       BooleanType(), True),
    StructField("ingested_at",      StringType(),  True),
    StructField("created_by_agent", StringType(),  True),
])

gold_df = spark.createDataFrame(enriched, schema=SCHEMA)
gold_df.orderBy(F.col("rpi").desc()) \
    .write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD)
spark.sql(f"ALTER TABLE {GOLD} SET TBLPROPERTIES ('created_by_agent' = 'true')")
print(f"Written {spark.sql(f'SELECT COUNT(*) AS n FROM {GOLD}').collect()[0]['n']:,} rows → {GOLD}")
```

```python
# Verify: top-10 + within_psr RPI check
print("=== TOP-10 RPI CELLS ===")
top10 = spark.sql(f"""
    SELECT cell_lat, cell_lon, pass_count,
           ROUND(setn_avg,2)      AS setn_avg,
           ROUND(n_suppression,4) AS nsi,
           ROUND(rpi,4)           AS rpi,
           nearest_psr_name,
           ROUND(distance_km,1)   AS dist_km,
           within_psr
    FROM {GOLD} ORDER BY rpi DESC LIMIT 10
""").collect()
for i, r in enumerate(top10, 1):
    tag = "IN PSR" if r["within_psr"] else "      "
    print(f"{i:2}. ({r['cell_lat']:8.4f}°,{r['cell_lon']:8.4f}°) "
          f"pass={r['pass_count']:2}  setn={r['setn_avg']:5.2f}  "
          f"NSI={r['nsi']:.4f}  RPI={r['rpi']:.4f}  "
          f"{r['nearest_psr_name']:<12} {r['dist_km']:6.1f}km {tag}")

check = spark.sql(f"""
    SELECT within_psr, ROUND(AVG(rpi),4) AS avg_rpi, COUNT(*) AS cells
    FROM {GOLD} GROUP BY within_psr ORDER BY within_psr DESC
""").collect()
print("\n=== RPI by PSR containment ===")
for r in check:
    label = "within PSR " if r["within_psr"] else "outside PSR"
    print(f"  {label}: avg_rpi={r['avg_rpi']}  cells={r['cells']:,}")
within_rpi  = next((r["avg_rpi"] for r in check if r["within_psr"]),  None)
outside_rpi = next((r["avg_rpi"] for r in check if not r["within_psr"]), None)
ok = within_rpi and outside_rpi and within_rpi > outside_rpi
print(f"\nWithin RPI > Outside RPI: {'PASS ✓' if ok else 'CHECK'}")
print(f"\n=== Gold v0.03 COMPLETE — {GOLD} ===")
```
