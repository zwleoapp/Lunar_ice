# Logic Notes — RPI Formula & LEND Normalization
<!-- created_by_agent -->

## Resource Potential Index (RPI)

### Formula

$$RPI = \frac{N_{suppression}}{S_{slope}}$$

Where:
- **N_suppression** — Normalized epithermal neutron suppression (dimensionless, 0–1 scale)
- **S_slope** — Terrain slope in degrees (from LOLA DEM)

**Interpretation:**
- High RPI → strong neutron suppression + low slope → high ice potential (accessible flat surface)
- Low RPI → weak suppression or steep terrain → low priority
- Threshold: RPI ≥ 1.5 flags a cell as a candidate region of interest

---

## LEND Neutron Suppression — Physical Basis

LRO LEND measures epithermal neutrons (0.4 eV – 1 MeV) escaping the lunar regolith.
Hydrogen atoms (from H₂O/OH) moderate fast neutrons, reducing the epithermal flux
escaping to orbit. This suppression is the primary neutron signature of near-surface ice.

**Detector channel used:** SETN — Sensor for Epithermal Thermal Neutrons (collimated)
- Field of view half-angle: ~5°
- Ground resolution: ~9 km at 50 km altitude
- Orbit repeat: ~2 h per polar pass

---

## Normalization — LEND PDS4 Release 65 (March 2026)

### Step 1 — Raw Count Rate
Each LEND product provides: `count_rate` (counts s⁻¹ cm⁻²) per integration interval.

### Step 2 — Equatorial Baseline Reference
The dry equatorial regolith (±30° latitude) represents the minimum-hydrogen reference.
Release 65 equatorial baseline (to be read from PDS4 label `LEND_SETN_CAL_PARAMS`):

| Parameter | Symbol | Expected Value* |
|---|---|---|
| Equatorial mean count rate | C_eq | ~1.20 counts s⁻¹ (collimated SETN) |
| 1-σ equatorial variability | σ_eq | ~0.05 counts s⁻¹ |
| Cosmic Ray Background | C_bg | read from `BACKGROUND_CORRECTION` field |

*Values marked with `*` must be confirmed by reading one Release 65 label file.
Do not hardcode; load from `config/targets.yaml` or from the PDS4 label at runtime.

### Step 3 — Suppression Calculation

```
N_suppression = max(0, (C_eq - C_observed) / C_eq)
```

Clipped to [0, 1]. Negative values (observed > equatorial) set to 0 (no suppression).

### Step 4 — Grid Cell Aggregation
Multiple LEND passes cover the same Shackleton grid cell. Aggregate per cell:
- `N_suppression_mean` — arithmetic mean across all valid passes
- `N_suppression_std` — standard deviation (data quality indicator)
- `pass_count` — number of orbits contributing to cell

### Step 5 — RPI Calculation

```
RPI = N_suppression_mean / S_slope_degrees
```

Edge cases:
- If `S_slope_degrees < 0.1`: clamp denominator to 0.1 (avoid division-by-zero on flat poles)
- If `pass_count < 3`: mark cell as `low_confidence = True`; exclude from Gold layer until more passes accumulate

---

## Terrain Slope (LOLA) — Phase 2 Input

LOLA DEM-derived slope will be ingested in Phase 2 at 60 m/pixel resolution.
The slope raster will be resampled to match the LEND ~9 km grid for RPI join.

Slope source: `LRO-L-LOLA-3-RDR-V1.0` — gridded DEM, 512 pixels/degree.

---

## Coordinate System

All data will be registered to:
- **Projection:** Simple Cylindrical (Equirectangular), Planetocentric
- **Datum:** Lunar Reconnaissance Orbiter Reference Frame (LRO_MOON_ME)
- **Origin:** center of Shackleton Crater (Lat: -89.9°, Lon: 0.0°)
- **Grid cell size:** 0.25° × 0.25° (~7.5 km near pole)

---

## Dataflow Summary

```
PDS4 Registry (pds.peppi)
    │
    ▼ label metadata (geographic filter)
Bronze: lunar_ice.south_pole.bronze_lend_metadata
    │
    ▼ download .dat files → raw_pds_blobs volume
    │  parse count_rate per integration
    │  apply C_bg correction
    │  compute N_suppression per pass
    ▼
Silver: lunar_ice.south_pole.silver_lend_suppression
    │
    ▼ aggregate per grid cell
    │  join with LOLA slope (Phase 2)
    │  compute RPI = N_suppression / S_slope
    ▼
Gold: lunar_ice.south_pole.gold_rpi_shackleton
    │
    ▼ filter RPI >= 1.5 (threshold from config/targets.yaml)
    ▼
Databricks App — Shackleton Ice Map
```

---

## References
- LEND PDS4 Archive: `LRO-L-LEND-2/3/4/5-*-V1.0` (NASA PDS Geosciences Node)
- Release 65 changelog: to be read from `CATALOG/LEND_RELEASE_NOTES.TXT` in archive
- Feldman et al. (2011) — LEND calibration methodology
- Mitrofanov et al. (2010) — LEND instrument description, *Science* 330
