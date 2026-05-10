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

## Gold v0.03 Confirmed Values (2026-05-10)

| Parameter | Value | Source |
|---|---|---|
| C_baseline | **12.9864 counts** | AVG(setn_total) over 71,327 Silver rows |
| C_stddev | 4.1197 | STDDEV(setn_total), June 2010 |
| mock_slope_deg | 2.0° | Config table; PSR floors typically 1–3° |
| Surviving cells | 11,734 | pass_count ≥ 3 filter on 23,933 total cells |
| Cells with positive NSI | 5,563 (47%) | setn_avg < C_baseline |
| Max NSI | 1.0 | setn_avg = 0 in 3-pass minimum cells |
| Max RPI | 0.5 | = NSI_max / mock_slope = 1.0 / 2.0 |
| Avg RPI | 0.033 | Most cells show weak suppression |
| Within-PSR avg RPI | 0.0336 vs 0.0329 | PSR cells score slightly higher ✓ |

**RPI threshold note:** The 1.5 target applies when real LOLA slopes replace mock_slope.
At mock_slope = 2.0°, max achievable RPI = 0.5. When LOLA floors at ~0.5° are used,
the same NSI = 1.0 cell yields RPI = 1.0 / 0.5 = 2.0 — above threshold. This is why
v0.04 LOLA ingestion is critical for the threshold to be meaningful.

---

## Gold v0.04 Confirmed Values (2026-05-10)

| Parameter | Value | Source |
|---|---|---|
| C_baseline_csetn | **2.9902 counts** | AVG(csetn1_total) over 71,327 Silver rows |
| Surviving CSETN cells | 11,734 | pass_count ≥ 3 filter — same as v0.03 SETN grid |
| Cells with positive NSI | 5,478 (47%) | csetn1_avg < C_baseline_csetn |
| LOLA slope range | 1.80°–33.54° | cell averages from ldem_85s_40m, 40m/pixel DEM |
| LOLA slope mean | 9.9° | south polar terrain is rugged |
| Max RPI_real | 0.3382 | NSI=1.0 / slope=2.96° |
| RPI threshold | 1.5 (unmet) | min cell slope = 1.8° → max RPI = 1.0/1.8 = 0.556 |

**RPI threshold analysis:** The 1.5 target requires slope ≤ NSI/1.5 ≤ 0.667°. At 0.25° cell resolution,
the minimum observed LOLA slope is 1.8° (cell-averaged). The threshold is meaningful only at pixel-level
(e.g., PGDA 5m/pix for Shackleton), not at LEND grid scale.

**LOLA DEM discovery:** The LOLA GDR is inside `lro-l-lola-3-rdr-v1/lrolol_1xxx/data/lola_gdr/polar/float_img/`
on pds-geosciences.wustl.edu — NOT a separate `lola-4-gdr` archive. File `ldem_85s_40m_float.img`:
7584 × 7584 pixels, float32 PC_REAL (little-endian), MAP_SCALE = 40 m/pixel, values in km.

---

## CSETN Grid Resolution — v0.04 Design Decision (2026-05-10)

CSETN footprint ≈ **9 km diameter** at 50 km LRO altitude. Grid must be matched to this physical limit.

| Grid | Lat cell size | Lon cell at −89° | Shackleton cells (21 km diam) | Verdict |
|---|---|---|---|---|
| 0.1° | 3 km | 53 m | ~7 lat | Over-resolved in lon; adjacent cells share same footprint |
| **0.25°** | **7.5 km** | **130 m** | **~3 lat** | **Chosen — sub-footprint via 379-pass averaging; aligns with v0.03 SETN grid** |
| 0.3° | 9.1 km | 157 m | ~2 lat | Nyquist-matched in latitude; physically rigorous |
| 0.5° | 15.2 km | 262 m | ~1 lat | Published LEND standard; too coarse for PSR interior |

**Decision: 0.25°** chosen for two reasons:
1. Direct SETN↔CSETN comparison: same cell boundaries as v0.03 — can diff suppression maps cell-by-cell.
2. 379-pass multi-pass averaging justifies sub-footprint resolution in latitude.

**Longitude caveat at deep pole:** At −89°S, 0.25° in longitude = ~130 m — 70× smaller than the
9 km footprint. Lon-adjacent cells near the geographic pole are physically correlated. This is
unavoidable for any grid finer than ~15° in longitude at −89°S. The scientific gains in v0.04 are
the CSETN signal sharpness (9 km vs 40 km) and real LOLA slope — not grid density.

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

## Terrain Slope (LOLA) — Phase 4 Complete

LOLA DEM slope ingested in Phase 4 at **40 m/pixel** resolution.
Source: `ldem_85s_40m_float.img` from `lro-l-lola-3-rdr-v1/lrolol_1xxx/data/lola_gdr/polar/float_img/`.
Result: `lunar_ice.south_pole.silver_lola_slopes` — 30,192 cells, slope range 1.80°–33.54°, mean 9.9°.
Joined to LEND 0.25° grid for Gold v0.04 RPI calculation. See `src/lola_silver_ingestion.md`.

---

## Coordinate System

All data will be registered to:
- **Projection:** Simple Cylindrical (Equirectangular), Planetocentric
- **Datum:** Lunar Reconnaissance Orbiter Reference Frame (LRO_MOON_ME)
- **Origin:** center of Shackleton Crater (Lat: -89.9°, Lon: 0.0°)
- **Grid cell size:** 0.25° × 0.25° (~7.5 km near pole)

---

## Dataflow Summary (updated 2026-05-10)

```
PDS3 Geosciences Node (pds-geosciences.wustl.edu)
    │
    ▼ index.tab (10.6 MB, 34,751 rows, fixed-width)
Bronze: lunar_ice.south_pole.bronze_lend_metadata
    │  28,729 science rows, partitioned by release_id
    │
    ▼ Phase filter (drop CRUISE, COMMISSIONING)
    │  Time slice (pilot: 2010-06-01 to 2010-06-03)
    │  Download .DAT files (binary, 594 bytes/row)
    │  Parse with Python struct (big-endian)
    │  Filter: lat <= -85.0° + POINTING + INTERSECTING flags
    ▼
Silver: lunar_ice.south_pole.silver_lend_targets  ✅ COMPLETE (71,327 rows — June 2010)
    │  One row per detector readout over south pole PSRs
    │  Columns: utc, orbit, lat, lon, alt, local_hour,
    │           setn_total, csetn1–4_total, binary_data_url
    │
Silver: lunar_ice.south_pole.silver_lola_slopes  ✅ COMPLETE (30,192 slope cells)
    │  LOLA ldem_85s_40m_float.img · 40m/pixel · numpy finite-difference slope
    │
    ▼ Aggregate per 0.25° grid cell
    │  Compute NSI = max(0, (C_baseline_csetn - csetn1_avg) / C_baseline_csetn)
    │  Join with LOLA real slope
    │  Compute RPI = NSI / max(slope_avg_deg, 0.1)
    ▼
Gold v0.04: lunar_ice.south_pole.gold_v4_shackleton_precision  ✅ COMPLETE
    │  11,734 cells · max RPI 0.338 · de Gerlache / Faustini / Shackleton ranked
    │  PSR cross-reference: 1,771 within-PSR cells · Haversine 7-PSR boundary check
    ▼
data/gold_v4_psr_rankings.csv  ✅ COMPLETE (890 KB · priority_tier + annotation)
    │  Published to github.com/zwleoapp/Lunar_ice (2026-05-10)
    ▼
Phase 5 — Full mission scale (Sept 2009–Sept 2010, ~875k rows)
    │  Chang'e-7 validation (July 2026+)
    ▼
Calibrated model → NASA extraction candidate selection
```

---

## Binary Row Parsing — LEND_RDR_RSCI.FMT (confirmed 2026-05-10)

Full spec: `config/lend_rsci_fmt.yaml`. Key findings from the FMT file:

### Row layout
- `ROW_BYTES = 594`, big-endian (MSB) throughout
- `IEEE_REAL` 8-byte fields → Python `struct` format `">d"` (big-endian double)
- `MSB_UNSIGNED_INTEGER` → `">B"` (1 byte), `">H"` (2 bytes), `">I"` (4 bytes)
- Spectrum fields are 16-bin arrays: `">16H"` (16 × 2-byte unsigned int)
- `CHARACTER` fields: raw bytes decoded as ASCII

### Science field byte offsets (1-indexed, Python offset = start_byte − 1)

| Field | Col | Start_byte | Bytes | Format | Notes |
|---|---|---|---|---|---|
| UTC | 3 | 13 | 23 | ASCII string | `yyyy-mm-ddThh:mm:ss.sss` |
| LRO_ORBIT_NUMBER | 5 | 40 | 4 | `">I"` | Increments at descending node |
| LATITUDE | 6 | 44 | 8 | `">d"` | Sub-spacecraft lat, lunar fixed |
| LONGITUDE | 7 | 52 | 8 | `">d"` | FMT typo: 'LONGITIUDE' |
| SCALT | 29 | 228 | 8 | `">d"` | Altitude in km (~50 km nominal) |
| LOCAL_HOUR | 30 | 236 | 1 | `">B"` | Local solar hour |
| LOCAL_MINUTE | 31 | 237 | 1 | `">B"` | Local solar minute |
| POINTING | 32 | 238 | 1 | `">B"` | Quality flag: 1 = valid pointing |
| INTERSECTING | 33 | 239 | 1 | `">B"` | Quality flag: 1 = boresight hits Moon |
| SETN_SPECTRUM | 37 | 275 | 32 | `">16H"` | 16-bin epithermal spectrum, sum for RPI |
| CSETN1–4_SPECTRUM | 40–43 | 371–467 | 32 each | `">16H"` | Collimated SETN, 9 km resolution |

### Quality filters applied in Silver
Keep only rows where `POINTING == 1` AND `INTERSECTING == 1`.
Rows with invalid pointing have zeroed position vectors and must not contribute to the spatial grid.

### SETN vs CSETN for RPI
- **SETN** (col 37): uncollimated, ~40 km ground resolution — broad mapping
- **CSETN1–4** (cols 40–43): collimated, ~9 km ground resolution — Shackleton-scale precision

Silver layer stores both. Gold layer uses CSETN for the PSR concentration map.

---

## Two-Tier PSR Geographic Gate

| Tier | Threshold | Target PSRs |
|---|---|---|
| Primary | lat ≤ −88.0° | Shackleton (−89.9°), de Gerlache (−88.3°) |
| Secondary | lat ≤ −85.0° | Nobile (−85.2°), Cabeus (−84.9°), Haworth (−87.5°), Faustini (−87.2°) |

Silver pilot uses −85° (captures all PSRs). Gold can filter further by tier and by PSR polygon.

**Cabeus note:** LCROSS 2009 confirmed water ice in Cabeus ejecta plume — highest-confidence ice
detection to date. LEND SETN measurements over Cabeus provide a ground-truth calibration anchor
for the RPI scale.

---

## Silver Chunk Strategy

| Chunk | Window | Files | Download | Rows | Status |
|---|---|---|---|---|---|
| Pilot | 2010-06-01 to 2010-06-03 | 3 | 153 MB | 7,209 | ✅ DONE |
| Month | 2010-06-01 to 2010-06-30 | 30 | 1.54 GB | 71,327 | ✅ DONE |
| Full nominal | 2009-09 to 2010-09 | ~365 | ~18 GB | ~875k | Phase 4+ |

June 2010 chosen: mid-nominal-mission, avoids commissioning artifacts, 
data quality flags stable, good south pole pass geometry.

---

## References
- LEND PDS4 Archive: `LRO-L-LEND-2/3/4/5-*-V1.0` (NASA PDS Geosciences Node)
- Release 65 changelog: to be read from `CATALOG/LEND_RELEASE_NOTES.TXT` in archive
- Feldman et al. (2011) — LEND calibration methodology
- Mitrofanov et al. (2010) — LEND instrument description, *Science* 330
