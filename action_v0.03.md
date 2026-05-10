# Action v0.03: The Gold Engine (Grid Aggregation & RPI)

## Science Mission Context
The final deliverable is a **ranked ice concentration map** of the lunar south pole to help NASA
identify extraction candidate sites. Water ice is most concentrated in Permanently Shadowed Regions
(PSRs) — deep crater floors that stay below −173°C and act as cold traps for volatiles.

China's Chang'e-7 deploys an in-situ ice detector at the south pole in **July 2026**. This Gold
layer produces the LEND-derived RPI map before that date. After Chang'e-7 publishes, confirmed
detections are cross-referenced against our RPI grid to validate and calibrate the model —
giving NASA a data-backed, externally validated shortlist for extraction planning.

---

## Architecture

```
Silver: lunar_ice.south_pole.silver_lend_targets
  71,327 rows · 379 orbits · June 2010 · lat ≤ -85°
  SETN mean = 12.99 counts · stddev = 4.12
         │
         ▼ Step 1: C_baseline
  C_baseline = AVG(setn_total) over all 71,327 rows ≈ 12.99
  → "dry" south pole background, saved to config/targets.yaml
         │
         ▼ Step 2: FLOOR grid binning
  cell_lat = FLOOR(lat  / 0.25) * 0.25 + 0.125   ← confirmed FLOOR method
  cell_lon = FLOOR(lon  / 0.25) * 0.25 + 0.125
  Distinct cells before filter: 23,933
  (out of 28,800 max possible = 83% coverage of lat ≤ -85° cap)
  Mean pass_count per cell ≈ 3.0  ← skewed: polar cells >> fringe cells
         │
         ▼ Step 3: Aggregate per cell + quality gate
  pass_count, setn_avg, setn_stddev, csetn1_avg
  DROP cells where pass_count < 3
  Expected survivors: cells near pole (heavy orbit convergence) reliably pass;
  fringe cells near -85° are sparse and many will be filtered — scientifically correct
         │
         ▼ Step 4: Neutron Suppression Index (NSI)
  N_suppression = MAX(0, (C_baseline - setn_avg) / C_baseline)
  Cells below mean → positive suppression → ice candidate signal
  Cells above mean → zero (no suppression, likely exposed regolith)
         │
         ▼ Step 5: Resource Potential Index (RPI)
  mock_slope_deg = 2.0°  ← PSR floors are typically 1–3°; 5° would deflate PSR scores
  RPI = N_suppression / MAX(mock_slope_deg, 0.1)
  Placeholder until LOLA DEM ingested in v0.04
         │
         ▼ Step 6: PSR Cross-Reference (Haversine)
  For each cell: spherical distance to each PSR centre (lunar radius 1737.4 km)
  → nearest_psr_name, distance_km, within_psr (distance ≤ PSR_radius_km)
  Source: config/spatial_bounds.yaml (7 PSRs defined)
         │
         ▼ Gold: lunar_ice.south_pole.gold_psr_rpi_rankings
  Ordered by RPI DESC · created_by_agent tag · TBLPROPERTIES set
  Top cells expected to cluster within Shackleton, de Gerlache, Faustini
```

**Key design decisions captured here:**
- **FLOOR binning (confirmed):** predictable boundaries, no ambiguity at 0.25° edges
- **C_baseline from Silver:** measures relative suppression within south polar zone — avoids
  needing separate equatorial ingestion; Cabeus (LCROSS confirmed ice) serves as anchor check
- **SETN for Gold v0.03:** broader 40 km footprint gives more stable per-cell statistics; CSETN1
  (9 km) stored as `csetn1_avg` for the v0.04 high-resolution Shackleton-precision map
- **mock_slope = 2.0°:** representative of PSR floors; replaced by LOLA DEM in v0.04
- **23,933 cells warning:** mean pass_count ≈ 3.0 means the pass_count ≥ 3 filter will be
  significant — many fringe cells at −85° will be dropped. This is correct behaviour: the
  map is most reliable (and most important) near the pole where orbit convergence is highest

---

## Strategic Context
**The Asset:** `silver_lend_targets` — 71,327 rows, 379 orbits, June 2010, all at lat ≤ −85°.
**The Goal:** Discrete 0.25° grid with RPI scores, PSR labels, and Haversine distances.
**The Risk:** Mean pass_count ≈ 3.0 per cell. The quality gate is tight — must confirm
surviving cell count is sufficient for a meaningful map before declaring Gold complete.

---

## Tasks

### Pre-Flight: [Sonnet] Sanity Check — Cell Count & Pass Distribution
- **Already known:** `COUNT(DISTINCT FLOOR(lat/0.25), FLOOR(lon/0.25))` = **23,933 cells**
  (confirmed by user query before action was approved)
- **Still to run in the notebook:** distribution of pass_count across cells to understand how
  many survive the `pass_count ≥ 3` gate:
  ```sql
  SELECT pass_count_bucket, COUNT(*) AS cells FROM (
    SELECT FLOOR(latitude/0.25)*10000 + FLOOR(longitude/0.25) AS cell_id,
           COUNT(*) AS pass_count_bucket
    FROM silver_lend_targets GROUP BY cell_id
  ) GROUP BY pass_count_bucket ORDER BY pass_count_bucket
  ```
- **Gate:** If surviving cells < 50, revisit quality threshold before proceeding.

### Task 1: [Sonnet] Compute & Persist C_baseline
- Query: `SELECT AVG(setn_total), STDDEV(setn_total), COUNT(*) FROM silver_lend_targets`
- Print result — expected ≈ 12.99 (mean) · 4.12 (stddev) · 71,327 (count)
- Write to `config/targets.yaml` under `lend.setn_baseline_june2010`
- **STOP and show output before Task 2.**

### Task 2: [Sonnet] Grid Binning & Aggregation
- Spark SQL grouping:
  ```sql
  SELECT
    FLOOR(latitude  / 0.25) * 0.25 + 0.125  AS cell_lat,
    FLOOR(longitude / 0.25) * 0.25 + 0.125  AS cell_lon,
    COUNT(*)                                  AS pass_count,
    AVG(setn_total)                           AS setn_avg,
    STDDEV(setn_total)                        AS setn_stddev,
    MIN(setn_total)                           AS setn_min,
    AVG(csetn1_total)                         AS csetn1_avg
  FROM silver_lend_targets
  GROUP BY cell_lat, cell_lon
  HAVING COUNT(*) >= 3
  ```
- Print surviving cell count and lat/lon extent.

### Task 3: [Sonnet] NSI + RPI Calculation
- `N_suppression = GREATEST(0, (C_baseline - setn_avg) / C_baseline)`
- `mock_slope_deg = 2.0` (constant; replaced by LOLA in v0.04)
- `RPI = N_suppression / GREATEST(mock_slope_deg, 0.1)`

### Task 4: [Sonnet] PSR Cross-Reference (Haversine)
- Moon radius: 1737.4 km
- Haversine formula in Python UDF or Spark SQL for distance from each cell centre to each
  PSR centre defined in `config/spatial_bounds.yaml`
- Add columns: `nearest_psr_name`, `distance_km`, `within_psr` (BOOLEAN)
- PSR list: Shackleton, de Gerlache, Haworth, Faustini, Sverdrup, Nobile, Cabeus

### Task 5: [Sonnet] Gold Notebook — Write & Verify
- Create `src/gold_rpi_mapping.md` combining Tasks 1–4 into a single Spark pipeline
- Write to `lunar_ice.south_pole.gold_psr_rpi_rankings` (Delta, ordered by RPI DESC)
- TBLPROPERTIES: `created_by_agent = true`
- Verify: print top-10 RPI cells with PSR label — expect Shackleton / de Gerlache / Faustini
- Verify: all `within_psr = true` cells have higher average RPI than `within_psr = false`

### Task 6: [Haiku] Artifact Update
- Update `notes/plan_notes.md`: Gold layer complete, Phase 4 LOLA DEM scope
- Update `notes/code_study_notes.md`: Gold design decisions (C_baseline, FLOOR, mock slope)
- Update `notes/logic_notes.md`: confirm C_baseline value from actual run
- Update `config/targets.yaml`: write confirmed `setn_baseline_june2010`

---

## Hard Gates
1. After **Pre-Flight + Task 1**: user sees cell distribution and C_baseline before notebook runs.
2. After **Task 5 verify**: user confirms top-10 cells correlate with known PSRs before declaring Gold complete.

---

## Success Criteria
- [x] Pre-flight confirms 23,933 distinct cells; pass distribution printed.
- [x] C_baseline computed live from Silver (12.9864), saved to `targets.yaml` + config table.
- [x] `gold_psr_rpi_rankings` created in Unity Catalog, ordered by RPI DESC.
- [x] Surviving cells ≥ 50 after `pass_count ≥ 3` filter — 11,734 cells survive.
- [x] `csetn1_avg`, `nearest_psr_name`, `distance_km`, `within_psr` columns present.
- [x] Top-ranked cells overlap with known PSRs (de Gerlache, Sverdrup, Shackleton, Faustini).
- [x] `within_psr = true` cells have higher mean RPI than `within_psr = false` (0.0336 vs 0.0329).
- [x] All code and tables carry the `created_by_agent` tag.
- [x] `config/targets.yaml` updated with `setn_baseline_june2010` and `mock_slope_deg`.
- [x] No hardcoded values — all params read from `lunar_ice.south_pole.config_gold_params`.

---

## Phase 4 Preview (out of scope for v0.03)
- **LOLA DEM slope ingestion:** replace `mock_slope_deg = 2.0` with real per-cell slope values
- **CSETN high-res map:** re-run Gold with `csetn1_avg` on a finer 0.1° grid for Shackleton precision
- **Full nominal mission Silver:** expand beyond June 2010 to Sept 2009 – Sept 2010 (~875k rows)
- **Chang'e-7 validation:** cross-reference confirmed ice sites against Gold RPI after July 2026
