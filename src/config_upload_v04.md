# Config Upload — Gold v0.04 (LOLA + CSETN)
<!-- created_by_agent -->

```python
import json
from pyspark.sql.types import StructType, StructField, StringType

CONFIG_TABLE = "lunar_ice.south_pole.config_gold_params"

GOLD_CONFIG_V04 = {
    "slope_clamp_deg":  0.1,
    "min_pass_count":   3,
    "rpi_threshold":    1.5,
    "moon_radius_km":   1737.4,
    "grid_deg":         0.25,
    "lat_limit":        -85.0,
    "silver_table":     "lunar_ice.south_pole.silver_lend_targets",
    "lola_slope_table": "lunar_ice.south_pole.silver_lola_slopes",
    "gold_table":       "lunar_ice.south_pole.gold_v4_shackleton_precision",
    "psrs": [
        {"name": "Shackleton",  "lat": -89.67, "lon": 129.47, "diameter_km":  21.0, "priority": 1},
        {"name": "de_Gerlache", "lat": -88.33, "lon": 272.01, "diameter_km":  32.0, "priority": 2},
        {"name": "Haworth",     "lat": -87.47, "lon": 355.33, "diameter_km":  51.0, "priority": 3},
        {"name": "Faustini",    "lat": -87.20, "lon":  83.25, "diameter_km":  43.0, "priority": 4},
        {"name": "Sverdrup",    "lat": -88.00, "lon": 152.50, "diameter_km":  33.0, "priority": 5},
        {"name": "Nobile",      "lat": -85.18, "lon":  53.53, "diameter_km":  73.0, "priority": 6},
        {"name": "Cabeus",      "lat": -84.90, "lon": 316.00, "diameter_km":  98.0, "priority": 7},
    ],
}

SCHEMA = StructType([
    StructField("config_key",       StringType(), False),
    StructField("config_json",      StringType(), True),
    StructField("created_by_agent", StringType(), True),
])

spark.createDataFrame(
    [("gold_v0.04", json.dumps(GOLD_CONFIG_V04), "true")],
    schema=SCHEMA,
).write.format("delta").mode("append").saveAsTable(CONFIG_TABLE)

spark.sql(f"ALTER TABLE {CONFIG_TABLE} SET TBLPROPERTIES ('created_by_agent' = 'true')")

rows = spark.sql(f"SELECT config_key FROM {CONFIG_TABLE} ORDER BY config_key").collect()
print("Config keys now in table:", [r["config_key"] for r in rows])
print("v0.04 config appended ✓")
```
