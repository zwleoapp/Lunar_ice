---
agent-notebook:
  language: python
---

# Config Upload — Write Gold config to Unity Catalog Delta table
<!-- created_by_agent -->
# Run once whenever config values change. Stores all Gold pipeline parameters
# in lunar_ice.south_pole.config_gold_params so notebooks have no hardcoded values.

```python
import json

CONFIG_TABLE = "lunar_ice.south_pole.config_gold_params"

GOLD_CONFIG = {
    "mock_slope_deg":    2.0,    # PSR floors 1–3°; replaced by LOLA DEM in v0.04
    "slope_clamp_deg":   0.1,    # denominator clamp to avoid division by zero
    "min_pass_count":    3,      # cells with fewer passes excluded
    "rpi_threshold":     1.5,    # candidate zone flag (meaningful with real LOLA slope)
    "moon_radius_km":    1737.4,
    "grid_deg":          0.25,
    "silver_table":      "lunar_ice.south_pole.silver_lend_targets",
    "gold_table":        "lunar_ice.south_pole.gold_psr_rpi_rankings",
    "psrs": [
        {"name": "Shackleton",  "lat": -89.67, "lon": 129.47, "diameter_km": 21.0,
         "priority": 1, "notes": "Primary target. ~4 km deep, permanently shadowed."},
        {"name": "de_Gerlache", "lat": -88.33, "lon": 272.01, "diameter_km": 32.0,
         "priority": 2, "notes": "Adjacent to Shackleton. High LEND ice signal."},
        {"name": "Haworth",     "lat": -87.47, "lon": 355.33, "diameter_km": 51.0,
         "priority": 3, "notes": "Large well-shadowed basin near Shackleton."},
        {"name": "Faustini",    "lat": -87.20, "lon":  83.25, "diameter_km": 43.0,
         "priority": 4, "notes": "Deep cold floor. Strong suppression candidate."},
        {"name": "Sverdrup",    "lat": -88.00, "lon": 152.50, "diameter_km": 33.0,
         "priority": 5, "notes": "High latitude, consistently shadowed."},
        {"name": "Nobile",      "lat": -85.18, "lon":  53.53, "diameter_km": 73.0,
         "priority": 6, "notes": "Large crater, NASA Artemis site proximity."},
        {"name": "Cabeus",      "lat": -84.90, "lon": 316.00, "diameter_km": 98.0,
         "priority": 7, "notes": "LCROSS 2009 confirmed water ice. RPI anchor."},
    ],
}

spark.createDataFrame(
    [("gold_v0.03", json.dumps(GOLD_CONFIG), "true")],
    ["config_key", "config_json", "created_by_agent"]
).write.format("delta").mode("overwrite") \
 .saveAsTable(CONFIG_TABLE)

spark.sql(f"ALTER TABLE {CONFIG_TABLE} SET TBLPROPERTIES ('created_by_agent' = 'true')")

row_count = spark.sql(f"SELECT COUNT(*) AS n FROM {CONFIG_TABLE}").collect()[0]["n"]
print(f"Config written to {CONFIG_TABLE} ({row_count} row)")

loaded = json.loads(
    spark.sql(f"SELECT config_json FROM {CONFIG_TABLE} WHERE config_key='gold_v0.03'")
         .collect()[0]["config_json"]
)
print(f"Verified: mock_slope={loaded['mock_slope_deg']}  PSRs={len(loaded['psrs'])}")
```
