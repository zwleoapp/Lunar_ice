---
agent-notebook:
  language: python
---

# Silver — Full June 2010 (30-day expansion)
<!-- created_by_agent -->

```python
import struct, requests, time, datetime
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, DoubleType, IntegerType)

LEND_BASE  = "https://pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/lrolen_0xxx"
BRONZE     = "lunar_ice.south_pole.bronze_lend_metadata"
TABLE      = "lunar_ice.south_pole.silver_lend_targets"
ROW_BYTES  = 594
LAT_GATE   = -85.0

candidates = [
    {"path_name": r["path_name"], "file_name": r["file_name"],
     "mission_phase": r["mission_phase"], "start_dt": r["start_dt"]}
    for r in spark.sql(f"""
        SELECT path_name, file_name, mission_phase, start_dt
        FROM {BRONZE}
        WHERE product_type  = 'LEND_RDR_RSCI'
          AND mission_phase NOT IN ('CRUISE', 'COMMISSIONING')
          AND start_dt >= '2010-06-01'
          AND start_dt <  '2010-07-01'
        ORDER BY start_dt
    """).collect()
]

print(f"Candidates : {len(candidates)} files")
phases = {c["mission_phase"] for c in candidates}
print(f"Phases     : {phases}")
print(f"First      : {candidates[0]['start_dt']}  →  {candidates[0]['file_name']}")
print(f"Last       : {candidates[-1]['start_dt']}  →  {candidates[-1]['file_name']}")
```

```python
def parse_row(b):
    utc      = b[12:35].decode("ascii", errors="replace").strip()
    orbit    = struct.unpack_from(">I", b, 39)[0]
    lat      = struct.unpack_from(">d", b, 43)[0]
    lon      = struct.unpack_from(">d", b, 51)[0]
    alt      = struct.unpack_from(">d", b, 227)[0]
    lochour  = b[235]
    locmin   = b[236]
    pointing = b[237]
    intersct = b[238]
    setn     = sum(struct.unpack_from(">16H", b, 274))
    csetn1   = sum(struct.unpack_from(">16H", b, 370))
    csetn2   = sum(struct.unpack_from(">16H", b, 402))
    csetn3   = sum(struct.unpack_from(">16H", b, 434))
    csetn4   = sum(struct.unpack_from(">16H", b, 466))
    return (utc, orbit, lat, lon, alt, lochour, locmin,
            pointing, intersct, setn, csetn1, csetn2, csetn3, csetn4)
```

```python
all_rows    = []
ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
total_raw   = 0

for idx, cand in enumerate(candidates, 1):
    file_dat      = cand["file_name"].replace(".LBL", ".DAT")
    product_id    = cand["file_name"].replace(".LBL", "")
    mission_phase = cand["mission_phase"]
    dat_url       = f"{LEND_BASE}/{cand['path_name']}{file_dat}"

    t0 = time.time()
    r  = requests.get(dat_url, stream=True, timeout=300)
    r.raise_for_status()
    data    = b"".join(r.iter_content(chunk_size=1 << 20))
    elapsed = time.time() - t0
    n_rows  = len(data) // ROW_BYTES
    total_raw += n_rows

    passing = 0
    for i in range(n_rows):
        b = data[i * ROW_BYTES : (i + 1) * ROW_BYTES]
        if len(b) < ROW_BYTES:
            break
        (utc, orbit, lat, lon, alt, lochour, locmin,
         pointing, intersct, setn, c1, c2, c3, c4) = parse_row(b)
        if lat <= LAT_GATE and pointing == 1 and intersct == 1:
            all_rows.append((
                product_id, utc, int(orbit),
                lat, lon, alt,
                int(lochour), int(locmin),
                int(setn), int(c1), int(c2), int(c3), int(c4),
                mission_phase, dat_url, ingested_at, "true"
            ))
            passing += 1

    print(f"[{idx:02d}/30] {file_dat}  {len(data)/1e6:.1f}MB {elapsed:.1f}s"
          f"  raw={n_rows}  passing={passing}  cumulative={len(all_rows)}")
    time.sleep(0.1)

print(f"\nTotal raw rows processed : {total_raw:,}")
print(f"Total rows after filter  : {len(all_rows):,}  ({100*len(all_rows)/total_raw:.2f}%)")
```

```python
SCHEMA_DEF = StructType([
    StructField("product_id",       StringType(),  True),
    StructField("utc",              StringType(),  True),
    StructField("orbit_number",     LongType(),    True),
    StructField("latitude",         DoubleType(),  True),
    StructField("longitude",        DoubleType(),  True),
    StructField("altitude_km",      DoubleType(),  True),
    StructField("local_hour",       IntegerType(), True),
    StructField("local_minute",     IntegerType(), True),
    StructField("setn_total",       IntegerType(), True),
    StructField("csetn1_total",     IntegerType(), True),
    StructField("csetn2_total",     IntegerType(), True),
    StructField("csetn3_total",     IntegerType(), True),
    StructField("csetn4_total",     IntegerType(), True),
    StructField("mission_phase",    StringType(),  True),
    StructField("binary_data_url",  StringType(),  True),
    StructField("ingested_at",      StringType(),  True),
    StructField("created_by_agent", StringType(),  True),
])

df = spark.createDataFrame(all_rows, schema=SCHEMA_DEF)
df.write.format("delta").mode("overwrite").partitionBy("mission_phase").saveAsTable(TABLE)
print(f"Written {df.count():,} rows to {TABLE}")
```

```python
total = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE}").collect()[0]["n"]

stats = spark.sql(f"""
    SELECT
        COUNT(DISTINCT product_id)          AS days,
        COUNT(DISTINCT orbit_number)        AS orbits,
        COUNT(*)                            AS total_rows,
        ROUND(MIN(latitude),  4)            AS lat_min,
        ROUND(MAX(latitude),  4)            AS lat_max,
        ROUND(AVG(setn_total), 2)           AS setn_mean,
        ROUND(STDDEV(setn_total), 2)        AS setn_stddev,
        ROUND(MIN(altitude_km), 1)          AS alt_min_km,
        ROUND(MAX(altitude_km), 1)          AS alt_max_km
    FROM {TABLE}
""").collect()[0]

print(f"Days    : {stats['days']}")
print(f"Orbits  : {stats['orbits']}")
print(f"Rows    : {stats['total_rows']:,}")
print(f"Lat     : {stats['lat_min']}° to {stats['lat_max']}°")
print(f"SETN    : mean={stats['setn_mean']}  stddev={stats['setn_stddev']}")
print(f"Alt     : {stats['alt_min_km']} – {stats['alt_max_km']} km")
print()

daily = spark.sql(f"""
    SELECT product_id,
           COUNT(*)                     AS rows,
           COUNT(DISTINCT orbit_number) AS orbits,
           ROUND(AVG(setn_total), 1)    AS setn_mean
    FROM {TABLE}
    GROUP BY product_id
    ORDER BY product_id
""")
display(daily)
print("\nSilver June 2010 full-month: COMPLETE")
```
