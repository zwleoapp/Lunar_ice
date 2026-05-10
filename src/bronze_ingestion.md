---
agent-notebook:
  language: python
---

# Bronze Ingestion — LEND PDS3 Index → bronze_lend_metadata
<!-- created_by_agent -->

```python
%pip install pyyaml --quiet
```

```python
import requests, yaml, datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Config via widget; fall back to defaults for import-check runs
try:
    CONFIG_PATH = dbutils.widgets.get("config_path")
    import pathlib
    cfg = yaml.safe_load(pathlib.Path(CONFIG_PATH).read_text())
except Exception:
    cfg = {"catalog": "lunar_ice", "schema": "south_pole", "volume": "raw_pds_blobs"}

CATALOG = cfg["catalog"]
SCHEMA  = cfg["schema"]
TABLE   = f"{CATALOG}.{SCHEMA}.bronze_lend_metadata"
print(f"Target table: {TABLE}")
```

```python
# PDS3 archive — confirmed by dry-run 2026-05-10
LEND_BASE = "https://pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/lrolen_0xxx"
INDEX_TAB = f"{LEND_BASE}/index/index.tab"

# Column layout from index.lbl (0-indexed start, byte count)
COLS = {
    "VOLUME_ID":                    (1,  11),
    "PATH_NAME":                    (15, 19),
    "FILE_NAME":                    (37, 31),
    "PRODUCT_ID":                   (71, 36),
    "PRODUCT_TYPE":                 (110, 27),
    "RELEASE_ID":                   (178, 4),
    "MISSION_PHASE_NAME":           (185, 31),
    "START_TIME":                   (229, 24),
    "STOP_TIME":                    (254, 24),
}

def col(row, name):
    s, n = COLS[name]
    return row[s:s+n].strip().strip('"').strip()

# Science product types — exclude housekeeping-only rows
SCI_TYPES = {"LEND_EDR_SCI", "LEND_RDR_CHK", "LEND_RDR_RSCI", "LEND_RDR_DLD", "LEND_RDR_DLX"}
```

```python
# Stream and parse index.tab
r = requests.head(INDEX_TAB, timeout=10)
print(f"index.tab: HTTP {r.status_code}  size={r.headers.get('Content-Length','?')} bytes")
r.raise_for_status()

records  = []
skipped  = 0
ingested = datetime.datetime.utcnow().isoformat() + "Z"

tab = requests.get(INDEX_TAB, timeout=300, stream=True)
tab.raise_for_status()

for raw in tab.iter_lines():
    row = raw.decode("latin-1").rstrip("\r\n")
    if not row:
        continue

    pt = col(row, "PRODUCT_TYPE")
    if pt not in SCI_TYPES:
        skipped += 1
        continue

    records.append((
        col(row, "VOLUME_ID"),
        col(row, "PATH_NAME"),
        col(row, "FILE_NAME"),
        col(row, "PRODUCT_ID"),
        pt,
        col(row, "RELEASE_ID"),
        col(row, "MISSION_PHASE_NAME"),
        col(row, "START_TIME"),
        col(row, "STOP_TIME"),
        ingested,
        "true",   # created_by_agent
    ))

print(f"Parsed: {len(records)} science rows  |  skipped (HK/other): {skipped}")
```

```python
# Write to Delta — batch in groups of 5,000 to stay within 2X-Small memory
SCHEMA_DEF = StructType([
    StructField("volume_id",        StringType(), True),
    StructField("path_name",        StringType(), True),
    StructField("file_name",        StringType(), True),
    StructField("product_id",       StringType(), True),
    StructField("product_type",     StringType(), True),
    StructField("release_id",       StringType(), True),
    StructField("mission_phase",    StringType(), True),
    StructField("start_dt",         StringType(), True),
    StructField("stop_dt",          StringType(), True),
    StructField("ingested_at",      StringType(), True),
    StructField("created_by_agent", StringType(), True),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        volume_id        STRING,
        path_name        STRING,
        file_name        STRING,
        product_id       STRING,
        product_type     STRING,
        release_id       STRING,
        mission_phase    STRING,
        start_dt         STRING,
        stop_dt          STRING,
        ingested_at      STRING,
        created_by_agent STRING
    )
    USING DELTA
    PARTITIONED BY (release_id)
    TBLPROPERTIES ('created_by_agent' = 'true')
""")

BATCH = 5000
written = 0
for i in range(0, len(records), BATCH):
    df = spark.createDataFrame(records[i:i+BATCH], schema=SCHEMA_DEF)
    df.write.format("delta").mode("append").saveAsTable(TABLE)
    written += df.count()
    print(f"  batch {i//BATCH+1}: wrote {df.count()} rows  (total {written})")

print(f"\nDone. {written} rows in {TABLE}")
```

```python
# Verify
count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE}").collect()[0]["n"]
sample = spark.sql(f"""
    SELECT product_type, COUNT(*) AS n
    FROM {TABLE}
    GROUP BY product_type
    ORDER BY n DESC
""")
display(sample)
print(f"\nTotal rows in {TABLE}: {count}")
print("Bronze ingestion: COMPLETE")
```
