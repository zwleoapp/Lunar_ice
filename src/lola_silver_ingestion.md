# LOLA Silver Ingestion — DEM → Slope → silver_lola_slopes
<!-- created_by_agent -->

```python
# ── Cell 1: Config + LBL Parse ──────────────────────────────────────────────
import json, requests, re, math, datetime
import numpy as np

CONFIG_TABLE = "lunar_ice.south_pole.config_gold_params"
cfg = json.loads(
    spark.sql(f"SELECT config_json FROM {CONFIG_TABLE} WHERE config_key='gold_v0.04'")
         .collect()[0]["config_json"]
)
SILVER_LOLA = cfg["lola_slope_table"]
GRID_DEG    = float(cfg["grid_deg"])
LAT_LIMIT   = float(cfg["lat_limit"])
R_KM        = float(cfg["moon_radius_km"])

H = {"User-Agent": "LunarIceResearch/1.0 zwapp@protonmail.com"}
LBL_URL = "https://pds-geosciences.wustl.edu/lro/lro-l-lola-3-rdr-v1/lrolol_1xxx/data/lola_gdr/polar/float_img/ldem_85s_40m_float.lbl"
IMG_URL = LBL_URL.replace('.lbl', '.img')

def parse_lbl(url):
    r = requests.get(url, headers=H, timeout=30)
    r.raise_for_status()
    kv = {}
    for line in r.text.splitlines():
        if '=' in line and not line.strip().startswith(('/*', 'END', '{')):
            k, _, v = line.partition('=')
            kv[k.strip()] = v.strip().strip('"').split('<')[0].strip()
    return kv

lbl = parse_lbl(LBL_URL)
print("Key LBL fields:")
for k in ['LINES','LINE_SAMPLES','SAMPLE_BITS','SAMPLE_TYPE','MAP_SCALE',
          'LINE_PROJECTION_OFFSET','SAMPLE_PROJECTION_OFFSET',
          'MAXIMUM_LATITUDE','MINIMUM_LATITUDE','MISSING_CONSTANT']:
    print(f"  {k}: {lbl.get(k,'—')}")

LINES        = int(lbl['LINES'])
LINE_SAMPLES = int(lbl['LINE_SAMPLES'])
SAMPLE_BITS  = int(lbl.get('SAMPLE_BITS', 32))
SAMPLE_TYPE  = lbl.get('SAMPLE_TYPE', 'PC_REAL')
PIXEL_KM     = float(lbl.get('MAP_SCALE', 40)) / 1000  # MAP_SCALE is m/pix in lbl; convert to km/pix
ROW_CENTER   = float(lbl.get('LINE_PROJECTION_OFFSET', (LINES - 1) / 2))
COL_CENTER   = float(lbl.get('SAMPLE_PROJECTION_OFFSET', (LINE_SAMPLES - 1) / 2))
NODATA       = float(lbl.get('MISSING_CONSTANT', -32768))
dtype        = '<f4' if 'PC' in SAMPLE_TYPE.upper() else '>f4'

print(f"\n  Size: {LINES} × {LINE_SAMPLES}  pixel={PIXEL_KM*1000:.0f}m  dtype={dtype}")
print(f"  Center: row={ROW_CENTER}, col={COL_CENTER}")
print(f"  Download: {LINES * LINE_SAMPLES * 4 / 1e6:.1f} MB")
```

```python
# ── Cell 2: Download DEM ─────────────────────────────────────────────────────
import time

print(f"Downloading {IMG_URL}")
t0 = time.time()
r = requests.get(IMG_URL, headers=H, timeout=600, stream=False)
print(f"  HTTP {r.status_code}  {len(r.content)/1e6:.1f} MB  {time.time()-t0:.1f}s")
assert r.status_code == 200, f"Download failed: {r.status_code}"

dem = np.frombuffer(r.content, dtype=dtype).reshape(LINES, LINE_SAMPLES).astype(np.float32)
dem[dem == NODATA] = np.nan
valid_pct = 100 * np.sum(~np.isnan(dem)) / dem.size
print(f"  Parsed {dem.shape}  valid pixels={valid_pct:.1f}%")
print(f"  Elevation: min={np.nanmin(dem):.0f}m  max={np.nanmax(dem):.0f}m")
```

```python
# ── Cell 3: Slope Computation + Cell Aggregation ─────────────────────────────
# Process in row chunks: compute slope, convert to lat/lon, bin to 0.25° cells
from collections import defaultdict

PIXEL_M    = PIXEL_KM * 1000  # metres per pixel
CHUNK_SIZE = 100               # rows per chunk (memory control)

cell_acc = defaultdict(lambda: [0.0, 0.0, 0])  # {(clat,clon): [sum, sum_sq, count]}

for r_start in range(0, LINES, CHUNK_SIZE):
    r_end = min(r_start + CHUNK_SIZE, LINES)
    # Include 1-row padding for gradient accuracy
    p0 = max(0, r_start - 1);  p1 = min(LINES, r_end + 1)
    chunk = dem[p0:p1].astype(np.float64)

    # Slope via finite differences (DEM is in km → ×1000 → metres; spacing in metres → dimensionless slope)
    gy_full, gx_full = np.gradient(chunk * 1000, PIXEL_M, PIXEL_M)
    a0 = r_start - p0;  a1 = a0 + (r_end - r_start)
    gy = gy_full[a0:a1];  gx = gx_full[a0:a1]
    slope_deg = np.degrees(np.arctan(np.sqrt(gx**2 + gy**2)))

    # Pixel coordinates → polar stereographic (km) → lat/lon
    rows_idx = np.arange(r_start, r_end, dtype=np.float64).reshape(-1, 1)
    cols_idx = np.arange(LINE_SAMPLES, dtype=np.float64).reshape(1, -1)
    x_km = (cols_idx - COL_CENTER) * PIXEL_KM   # east  (+)
    y_km = (ROW_CENTER - rows_idx) * PIXEL_KM   # north (+)
    rho_km = np.sqrt(x_km**2 + y_km**2)
    lat = -90.0 + 2.0 * np.degrees(np.arctan(rho_km / (2.0 * R_KM)))
    lon = np.degrees(np.arctan2(x_km, y_km))
    lon[lon < 0] += 360.0

    valid = (~np.isnan(slope_deg)) & (lat <= LAT_LIMIT + 0.01) & (slope_deg >= 0)

    # FLOOR cell assignment (vectorized)
    clat = np.floor(lat / GRID_DEG) * GRID_DEG + GRID_DEG / 2
    clon = np.floor(lon / GRID_DEG) * GRID_DEG + GRID_DEG / 2

    # np.bincount accumulation: encode (clat, clon) as integer key
    clat_i = np.round(clat * 1000).astype(np.int32)   # e.g. -85125
    clon_i = np.round(clon * 1000).astype(np.int32)   # e.g.  129125
    key_arr = clat_i.ravel().astype(np.int64) * 1_000_000_000 + clon_i.ravel().astype(np.int64)
    slope_flat = slope_deg.ravel()
    valid_flat = valid.ravel()

    v_keys  = key_arr[valid_flat]
    v_slope = slope_flat[valid_flat]
    ukeys, inv = np.unique(v_keys, return_inverse=True)
    sums    = np.bincount(inv, weights=v_slope, minlength=len(ukeys))
    sums_sq = np.bincount(inv, weights=v_slope**2, minlength=len(ukeys))
    cnts    = np.bincount(inv, minlength=len(ukeys))

    for i, uk in enumerate(ukeys):
        clat_v = round((uk // 1_000_000_000) / 1000, 3)
        clon_v = round((uk % 1_000_000_000) / 1000, 3)
        cell_acc[(clat_v, clon_v)][0] += sums[i]
        cell_acc[(clat_v, clon_v)][1] += sums_sq[i]
        cell_acc[(clat_v, clon_v)][2] += cnts[i]

    if r_start % 1000 == 0:
        print(f"  rows {r_start}/{LINES}  cells so far: {len(cell_acc):,}")

print(f"\nDone. Total 0.25° cells: {len(cell_acc):,}")
```

```python
# ── Cell 4: Write silver_lola_slopes + Verify ────────────────────────────────
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.sql import functions as F

ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
rows = []
for (clat, clon), (s, s2, n) in cell_acc.items():
    if n == 0:
        continue
    mean = s / n
    std  = math.sqrt(max(0.0, s2 / n - mean**2)) if n > 1 else 0.0
    rows.append((float(clat), float(clon), round(mean, 4), round(std, 4), int(n), ingested_at, "true"))

SCHEMA = StructType([
    StructField("cell_lat",         DoubleType(),  False),
    StructField("cell_lon",         DoubleType(),  False),
    StructField("slope_avg_deg",    DoubleType(),  True),
    StructField("slope_std_deg",    DoubleType(),  True),
    StructField("pixel_count",      IntegerType(), True),
    StructField("ingested_at",      StringType(),  True),
    StructField("created_by_agent", StringType(),  True),
])

lola_df = spark.createDataFrame(rows, schema=SCHEMA)
lola_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(SILVER_LOLA)
spark.sql(f"ALTER TABLE {SILVER_LOLA} SET TBLPROPERTIES ('created_by_agent' = 'true')")

n_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {SILVER_LOLA}").collect()[0]["n"]
stats = spark.sql(f"""
    SELECT ROUND(MIN(cell_lat),3) AS lat_min, ROUND(MAX(cell_lat),3) AS lat_max,
           ROUND(AVG(slope_avg_deg),3) AS slope_mean,
           ROUND(MIN(slope_avg_deg),3) AS slope_min,
           ROUND(MAX(slope_avg_deg),3) AS slope_max,
           SUM(CASE WHEN slope_avg_deg <= 2.0 THEN 1 ELSE 0 END) AS flat_cells
    FROM {SILVER_LOLA}
""").collect()[0]
print(f"Written {n_rows:,} rows → {SILVER_LOLA}")
print(f"  Lat: {stats['lat_min']}° to {stats['lat_max']}°")
print(f"  Slope mean={stats['slope_mean']}°  min={stats['slope_min']}°  max={stats['slope_max']}°")
print(f"  Cells with slope ≤ 2° (PSR floor targets): {stats['flat_cells']:,}")
print("\n=== silver_lola_slopes COMPLETE ===")
```
