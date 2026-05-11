---
agent-notebook:
  language: python
---

# Silver Transformation — LEND Binary Parse → silver_lend_targets
<!-- created_by_agent -->

```python
import csv, struct, pathlib, requests, time, datetime
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, DoubleType, IntegerType)

LEND_BASE   = "https://pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/lrolen_0xxx"
CSV_PATH    = pathlib.Path("/Users/gwu/Documents/Lunar_ice/config/june2010_3day_candidates.csv")
ROW_BYTES   = 594
LAT_GATE    = -85.0
TABLE       = "lunar_ice.south_pole.silver_lend_targets"

with CSV_PATH.open() as f:
    candidates = list(csv.DictReader(f))

print(f"Candidates : {len(candidates)} files")
for c in candidates:
    print(f"  {c['file_name']}  |  {c['start_dt']}  |  {c['mission_phase']}")
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
# Serial download + parse — time.sleep(0.1) between files to be polite to USGS server
all_rows    = []
ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

for cand in candidates:
    path_name    = cand["path_name"]
    file_dat     = cand["file_name"].replace(".LBL", ".DAT")
    product_id   = cand["file_name"].replace(".LBL", "")
    mission_phase = cand["mission_phase"]
    dat_url      = f"{LEND_BASE}/{path_name}{file_dat}"

    print(f"\nDownloading {file_dat} ...")
    t0 = time.time()
    r = requests.get(dat_url, stream=True, timeout=300)
    r.raise_for_status()
    data = b"".join(r.iter_content(chunk_size=1 << 20))
    elapsed = time.time() - t0
    n_rows  = len(data) // ROW_BYTES
    print(f"  {len(data)/1e6:.1f} MB in {elapsed:.1f}s  —  {n_rows} raw rows")

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

    print(f"  Rows passing lat <= {LAT_GATE}° + quality flags : {passing}")
    time.sleep(0.1)

print(f"\nTotal rows collected across all files : {len(all_rows)}")
```

```python
# Write to Delta
SCHEMA_DEF = StructType([
    StructField("product_id",      StringType(),  True),
    StructField("utc",             StringType(),  True),
    StructField("orbit_number",    LongType(),    True),
    StructField("latitude",        DoubleType(),  True),
    StructField("longitude",       DoubleType(),  True),
    StructField("altitude_km",     DoubleType(),  True),
    StructField("local_hour",      IntegerType(), True),
    StructField("local_minute",    IntegerType(), True),
    StructField("setn_total",      IntegerType(), True),
    StructField("csetn1_total",    IntegerType(), True),
    StructField("csetn2_total",    IntegerType(), True),
    StructField("csetn3_total",    IntegerType(), True),
    StructField("csetn4_total",    IntegerType(), True),
    StructField("mission_phase",   StringType(),  True),
    StructField("binary_data_url", StringType(),  True),
    StructField("ingested_at",     StringType(),  True),
    StructField("created_by_agent",StringType(),  True),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        product_id       STRING,
        utc              STRING,
        orbit_number     BIGINT,
        latitude         DOUBLE,
        longitude        DOUBLE,
        altitude_km      DOUBLE,
        local_hour       INT,
        local_minute     INT,
        setn_total       INT,
        csetn1_total     INT,
        csetn2_total     INT,
        csetn3_total     INT,
        csetn4_total     INT,
        mission_phase    STRING,
        binary_data_url  STRING,
        ingested_at      STRING,
        created_by_agent STRING
    )
    USING DELTA
    PARTITIONED BY (mission_phase)
    TBLPROPERTIES ('created_by_agent' = 'true')
""")

df = spark.createDataFrame(all_rows, schema=SCHEMA_DEF)
df.write.format("delta").mode("overwrite").partitionBy("mission_phase").saveAsTable(TABLE)
written = df.count()
print(f"Written {written} rows to {TABLE}")
```

```python
# Verify
total = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE}").collect()[0]["n"]

stats = spark.sql(f"""
    SELECT
        product_id,
        COUNT(*)                    AS rows,
        ROUND(MIN(latitude), 4)     AS lat_min,
        ROUND(MAX(latitude), 4)     AS lat_max,
        ROUND(MIN(longitude), 2)    AS lon_min,
        ROUND(MAX(longitude), 2)    AS lon_max,
        ROUND(AVG(setn_total), 1)   AS setn_mean,
        ROUND(AVG(csetn1_total), 1) AS csetn1_mean
    FROM {TABLE}
    GROUP BY product_id
    ORDER BY product_id
""")
display(stats)

print(f"\nTotal rows in {TABLE}: {total}")
print("Silver transformation: COMPLETE")
```
