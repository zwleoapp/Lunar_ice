---
agent-notebook:
  language: python
---

# Smoke Test — Databricks Connectivity

```python
print("=== Spark Version ===")
print(spark.version)
```

```python
print("=== Available Catalogs ===")
catalogs = spark.sql("SHOW CATALOGS").collect()
for row in catalogs:
    print(f"  {row[0]}")
```

```python
print("=== Done ===")
