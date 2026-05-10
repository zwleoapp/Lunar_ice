---
agent-notebook:
  language: python
---

# Task 1: Workspace Exploration — Lunar Ice Catalog & Schema Setup

```python
import json
from datetime import datetime

findings = {}
findings["timestamp"] = datetime.utcnow().isoformat() + "Z"
findings["spark_version"] = spark.version
print(f"Spark version: {spark.version}")
```

```python
# Confirm lunar_ice catalog exists
catalogs = [row[0].lower() for row in spark.sql("SHOW CATALOGS").collect()]
findings["catalogs"] = catalogs
findings["lunar_ice_exists"] = "lunar_ice" in catalogs
print(f"All catalogs: {catalogs}")
print(f"lunar_ice catalog exists: {findings['lunar_ice_exists']}")
```

```python
# Check/create south_pole schema
if findings["lunar_ice_exists"]:
    spark.sql("USE CATALOG lunar_ice")
    schemas = [row[0].lower() for row in spark.sql("SHOW SCHEMAS").collect()]
    findings["schemas"] = schemas
    findings["south_pole_exists"] = "south_pole" in schemas
    print(f"Schemas in lunar_ice: {schemas}")

    if not findings["south_pole_exists"]:
        spark.sql("""
            CREATE SCHEMA IF NOT EXISTS lunar_ice.south_pole
            COMMENT 'South Pole region — Lunar Ice Explorer. created_by_agent'
        """)
        findings["south_pole_created"] = True
        print("ACTION: Created schema lunar_ice.south_pole")
    else:
        findings["south_pole_created"] = False
        print("OK: south_pole schema already exists")
else:
    findings["schemas"] = []
    findings["south_pole_exists"] = False
    findings["south_pole_created"] = False
    print("ERROR: lunar_ice catalog not found")
```

```python
# Check for raw_pds_blobs volume
findings["raw_pds_blobs_exists"] = False
findings["volumes"] = []
try:
    volumes = [row[2].lower() for row in spark.sql("SHOW VOLUMES IN lunar_ice.south_pole").collect()]
    findings["volumes"] = volumes
    findings["raw_pds_blobs_exists"] = "raw_pds_blobs" in volumes
    print(f"Volumes in lunar_ice.south_pole: {volumes}")
    print(f"raw_pds_blobs exists: {findings['raw_pds_blobs_exists']}")
except Exception as e:
    findings["volume_check_error"] = str(e)
    print(f"Volume check: {e}")
```

```python
# Summary table for notebook output
display(spark.createDataFrame([
    ("Spark Version",       findings["spark_version"]),
    ("lunar_ice catalog",   str(findings["lunar_ice_exists"])),
    ("south_pole schema",   str(findings["south_pole_exists"])),
    ("south_pole created",  str(findings.get("south_pole_created", False))),
    ("raw_pds_blobs volume",str(findings["raw_pds_blobs_exists"])),
    ("volumes found",       str(findings["volumes"])),
], ["check", "result"]))
```

```python
# Final JSON dump — captured by agent for notes/workspace_status.txt
print("FINDINGS_JSON_START")
print(json.dumps(findings, indent=2))
print("FINDINGS_JSON_END")
```
