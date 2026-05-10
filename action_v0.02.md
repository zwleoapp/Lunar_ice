# Action v0.02 (Revised): The Silver Strike — Binary Processing

## Science Mission Context
The final deliverable of this project is a **ranked ice concentration map** of the lunar south pole
to help NASA identify extraction candidate sites. The working hypothesis: water ice is most
concentrated in Permanently Shadowed Regions (PSRs) — deep crater floors that never receive direct
sunlight, stay below -173°C, and act as cold traps for volatiles.

China's Chang'e-7 mission deploys a flying detector at the south pole in **July 2026**. Having a
LEND-derived ice probability map ready before then gives NASA a data-backed shortlist of PSR zones
to prioritise for extraction planning.

**Key PSRs within the lat ≤ -85° capture zone:**

| PSR | Approx. Lat | Notes |
|---|---|---|
| Shackleton | -89.9° | Primary target, ~21 km diameter |
| Cabeus | -84.9° | LCROSS 2009 impact — water confirmed |
| Haworth | -87.5° | Large, well-shadowed |
| Nobile | -85.2° | High ice probability from LEND surveys |
| de Gerlache | -88.3° | Adjacent to Shackleton |
| Faustini | -87.2° | Deep, cold floor |

The Gold layer (Phase 3) will grid these regions at 0.25° resolution and rank by RPI score,
producing a shortlist of extraction candidate cells.

---

## What Changed From Original v0.02
The Tracer Bullet test (2026-05-10) proved `.LBL` label files carry no lat/lon coordinates.
Label scraping is dropped entirely. Architecture is now:
- **Option A:** Mission phase filter (drop CRUISE, COMMISSIONING)
- **Option C:** Download binary `.DAT` files, parse measurement rows with Python `struct`,
  apply lat gate on actual detector readings

**Completed work that stands:**
- `config/rsci_paths.csv` — 6,015 unique RSCI paths (candidate pool)
- `src/path_distillation.md` — done
- `src/test_label_scraper.py` — superseded

---

## Objective
Produce `lunar_ice.south_pole.silver_lend_targets`: individual LEND SETN detector readout rows
where the spacecraft latitude was ≤ -85.0° (capturing all major south pole PSRs), sourced from a
3-day pilot window in June 2010 (nominal mission, south pole fully in shadow season).

---

## Pilot Scope

| Parameter | Pilot Value | Full-run Value |
|---|---|---|
| Product type | `LEND_RDR_RSCI` | Same |
| Date window | 2010-06-01 to 2010-06-03 | Expand after validation |
| Files | ~3 | ~30 per month |
| Download budget | ~153 MB | ~1.5 GB/month |
| Lat gate | ≤ -85.0° (wide — all major PSRs) | Tighten to ≤ -88.0° for Shackleton focus |
| Rows expected after filter | ~7,200 | ~72,000/month |

---

## Architecture

```
Bronze table                    FMT spec (USGS)
(28,729 metadata rows)          LEND_RDR_RSCI.FMT
        |                              |
  Phase filter                  Column map
  (drop CRUISE,                 (byte offsets for
   COMMISSIONING)               lat, lon, UTC time,
        |                        SETN neutron count)
  Time slice                         |
  2010-06-01 to                      |
  2010-06-03 (pilot)                 |
        |                            |
  ~3 candidate                       |
  product rows                       |
        |                            |
        +----------+-----------------+
                   |
         Download .DAT files (~3 × ~51 MB)
         Parse binary rows via Python struct
         Filter: spacecraft lat ≤ -85.0°
         (~2,400 rows survive per file)
                   |
         Silver Delta table
         lunar_ice.south_pole.silver_lend_targets
         (feeds Gold RPI grid → PSR ranked map)
```

---

## Tasks

### Pre-Task: [Haiku Subagent] FMT Spec Discovery
**MUST complete before any binary parsing code is written.**
- Fetch `LEND_RDR_RSCI.FMT` from:
  `https://pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/lrolen_0xxx/LABEL/LEND_RDR_RSCI.FMT`
- Parse all 46 column definitions: NAME, START_BYTE, BYTES, DATA_TYPE.
- Identify exact byte offsets for: latitude, longitude, UTC time, SETN count.
- Save column map to `config/lend_rsci_fmt.yaml`.
- Document in `notes/logic_notes.md`.

### Task 1: [Haiku Subagent] Phase + Time Filter — 3-Day Pilot Candidate List
- Query `lunar_ice.south_pole.bronze_lend_metadata`:
  ```sql
  SELECT path_name, file_name, start_dt, stop_dt, mission_phase
  FROM bronze_lend_metadata
  WHERE product_type = 'LEND_RDR_RSCI'
    AND mission_phase NOT IN ('CRUISE', 'COMMISSIONING')
    AND start_dt >= '2010-06-01'
    AND start_dt <  '2010-06-04'
  ORDER BY start_dt
  ```
- Write results to `config/june2010_3day_candidates.csv`.
- Print row count and distinct mission phases.
- **STOP and report before proceeding.**

### Task 2: [Sonnet] Tracer Bullet — Single DAT File
**STOP after this task. Wait for user approval before Task 3.**
- Write `src/test_binary_parser.py` (local Python script).
- Read first row of `config/june2010_3day_candidates.csv`.
- Construct `.DAT` URL: `LEND_BASE / path_name / file_name.replace(".LBL", ".DAT")`
- Download full `.DAT` file (stream to temp file).
- Parse every row using Python `struct` and `config/lend_rsci_fmt.yaml`.
- Extract per-row: `(utc_time, latitude, longitude, setn_count)`.
- Filter rows: `latitude ≤ -85.0`.
- Print: total rows parsed, rows passing filter, lat/lon range, SETN count range.
- Print 5 sample passing rows in human-readable form.
- **Identify which PSR(s) the passing rows fall within, if determinable from lon.**

### Task 3: [Sonnet] Silver Transformation Notebook
- Create `src/silver_transformation.md` (Databricks agent-notebook).
- **Step 1:** Read `config/june2010_3day_candidates.csv` into a Python list.
- **Step 2 — Serial download + parse loop** (`time.sleep(0.1)` between files):
  - Per file: download `.DAT`, parse binary rows, filter `latitude ≤ -85.0`.
  - Accumulate filtered rows in memory.
- **Step 3 — Spark write:**
  - Schema: `(product_id, utc_time, latitude, longitude, setn_count, mission_phase,
    binary_data_url, ingested_at, created_by_agent)`.
  - Write to `lunar_ice.south_pole.silver_lend_targets` (Delta, mode=overwrite).
  - Partition by `mission_phase`.
  - TBLPROPERTIES: `created_by_agent = true`.
- **Step 4 — Verify:** Print row count, lat/lon range, SETN count statistics.

### Task 4: [Config] Update Config Files
- Create `config/spatial_bounds.yaml`:
  ```yaml
  # created_by_agent
  shackleton:
    primary_lat_max: -88.0     # Primary: orbit crosses Shackleton / de Gerlache
    secondary_lat_max: -85.0   # Secondary: all major PSRs (Cabeus, Nobile, Haworth, Faustini)
    lon_range: [0, 360]
  silver_chunk:
    pilot_start: "2010-06-01"
    pilot_end:   "2010-06-03"
    mission_phase_exclude: ["CRUISE", "COMMISSIONING"]
  ```
- Update `config/targets.yaml`: add `lend.primary_product: LEND_RDR_RSCI`.

### Task 5: [Logic Subagent] Update logic_notes.md
- Add: **Binary Row Parsing** — struct format string, byte layout, endianness.
- Add: **Two-Tier PSR Gate** — -88.0° Primary (Shackleton/de Gerlache), -85.0° Secondary (all PSRs).
- Add: **Silver Chunk Strategy** — June 2010 pilot rationale, expansion path.
- Add: **PSR Inventory** — the 6 key PSRs, their coordinates, science priority.

### Task 6: [Haiku Subagent] Efficiency Chronicling
- Update `notes/code_study_notes.md`.
- Document: label scraping dropped (tracer bullet), binary parse architecture, polite serial
  download strategy, PSR science context, Chang'e-7 July 2026 urgency, and how the Silver
  lat filter directly maps to PSR cold trap boundaries.

---

## Hard Gates
1. After **Pre-Task**: user confirms FMT column map before parser is written. ✅ PASSED
2. After **Task 1**: user confirms 3-day candidate list looks right. ✅ PASSED
3. After **Task 2**: user confirms Tracer Bullet lat/lon/SETN output is plausible. ✅ PASSED

---

## Success Criteria
- [x] `config/lend_rsci_fmt.yaml` — all 46 columns with byte offsets and types.
- [x] `config/june2010_3day_candidates.csv` — 3 nominal-mission RSCI paths confirmed.
- [x] `src/test_binary_parser.py` — single `.DAT` parsed, 2,457 rows at lat ≤ −85° (user-verified).
- [x] `lunar_ice.south_pole.silver_lend_targets` — created in Unity Catalog, 7,209 pilot rows.
- [x] Silver rows: `latitude ≤ -85.0`, `binary_data_url` populated, all 5 neutron channels stored.
- [x] All new code and tables carry the `created_by_agent` tag.
- [x] `config/spatial_bounds.yaml` — PSR inventory, two-tier lat gates, Chang'e-7 validation plan.
- [x] `config/targets.yaml` — `primary_product: LEND_RDR_RSCI`, collimated channel noted.
- [x] `notes/logic_notes.md` — binary parse spec, PSR gate table, updated dataflow with Chang'e-7.
- [x] `notes/code_study_notes.md` — label scraping pivot, Option A+C, polite scrape rationale.
- [x] `notes/plan_notes.md` — full medallion status, phase pivot log, Phase 3 roadmap.

---

## Action v0.02 Status: COMPLETE ✅
**Completed:** 2026-05-10

### Final Silver State (June 2010 full month)
| Metric | Value |
|---|---|
| Days | 30 |
| Orbits | 379 |
| Raw rows processed | 2,592,000 |
| Rows in silver_lend_targets | 71,327 (2.75% pass rate) |
| Lat range | −89.9952° to −85.0000° |
| SETN mean | 12.99 counts · stddev 4.12 |
| Altitude range | 41.3 – 71.2 km |
| Runtime | 480s, zero USGS errors |
| Grid cell coverage | ~10–13 passes per 0.25° cell → above pass_count ≥ 3 threshold |

**Next action:** `action_v0.03.md` — Phase 3 Gold: aggregate Silver SETN counts per 0.25° grid,
compute RPI, join LOLA slope, produce ranked PSR ice concentration map for NASA extraction targeting.
Validate predictions against Chang'e-7 dataset after July 2026 release.
