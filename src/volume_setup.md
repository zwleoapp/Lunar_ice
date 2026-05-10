---
agent-notebook:
  language: python
---

# Volume Setup — raw_pds_blobs
<!-- created_by_agent -->

```python
CATALOG = "lunar_ice"
SCHEMA  = "south_pole"
VOLUME  = "raw_pds_blobs"
print(f"Target: {CATALOG}.{SCHEMA} / volume={VOLUME}")
```

```python
def list_volumes(catalog, schema):
    """Use information_schema for reliable column access."""
    rows = spark.sql(f"""
        SELECT volume_name
        FROM {catalog}.information_schema.volumes
        WHERE volume_schema = '{schema}'
    """).collect()
    return [row[0].lower() for row in rows]

existing = list_volumes(CATALOG, SCHEMA)
print(f"Existing volumes: {existing}")
```

```python
if VOLUME not in existing:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}
        COMMENT 'Raw PDS4 blob storage for LRO LEND and LOLA files. created_by_agent'
    """)
    print(f"ACTION: Created volume {CATALOG}.{SCHEMA}.{VOLUME}")
else:
    print(f"OK: volume {VOLUME} already exists")
```

```python
volumes_after = list_volumes(CATALOG, SCHEMA)
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(f"Volumes now: {volumes_after}")
print(f"Volume path: {volume_path}")
print(f"raw_pds_blobs present: {'raw_pds_blobs' in volumes_after}")
```
