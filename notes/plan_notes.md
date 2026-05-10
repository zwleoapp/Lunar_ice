# Plan Notes — Lunar Ice Explorer
<!-- created_by_agent -->

## Mission
Produce a ranked ice concentration map of the lunar south pole to help NASA identify
extraction candidate sites. Hypothesis: water ice is trapped in Permanently Shadowed Regions
(PSRs) where temperatures stay below −173°C and volatiles accumulate over billions of years.

The LEND SETN neutron suppression signal is the primary remote sensing proxy for hydrogen
(water ice). Combined with LOLA terrain slope, the RPI score identifies flat, high-suppression
cells most accessible for extraction.

**External milestone:** China's Chang'e-7 mission (July 2026) deploys an in-situ ice detector
at the south pole. Once that dataset is published, Gold-layer RPI predictions can be validated
against Chang'e-7 confirmed detections — giving NASA a calibrated, data-backed shortlist.

---

## Medallion Architecture — Current State

### Bronze — `lunar_ice.south_pole.bronze_lend_metadata` ✅ COMPLETE
- 28,729 science rows from PDS3 `index.tab` (Release 65, lrolen_0xxx)
- Columns: `volume_id`, `path_name`, `file_name`, `product_id`, `product_type`,
  `release_id`, `mission_phase`, `start_dt`, `stop_dt`, `ingested_at`, `created_by_agent`
- Partition: `release_id`
- Notebook: `src/bronze_ingestion.md` — runs in ~90s, <1 DBU

### Silver — `lunar_ice.south_pole.silver_lend_targets` ✅ JUNE 2010 COMPLETE
- **71,327 rows** from full June 2010 (2010-06-01 to 2010-06-30), `LEND_RDR_RSCI` product
- One row per LEND detector readout where spacecraft lat ≤ −85° + quality flags
- 379 orbits · 30 days · 2,592,000 raw rows processed (2.75% pass rate)
- SETN mean 12.99 counts · stddev 4.12 · lat range −89.9952° to −85.0°
- Columns: `product_id`, `utc`, `orbit_number`, `latitude`, `longitude`, `altitude_km`,
  `local_hour`, `local_minute`, `setn_total`, `csetn1–4_total`, `mission_phase`,
  `binary_data_url`, `ingested_at`, `created_by_agent`
- Partition: `mission_phase` · Notebooks: `src/silver_transformation.md` (pilot),
  `src/silver_june2010_full.md` (production)
- Grid cell coverage: ~10–13 passes per 0.25° cell → above confidence threshold
- Ready for Gold aggregation

### Gold — `lunar_ice.south_pole.gold_psr_rpi_rankings` ✅ v0.03 COMPLETE
- **11,734 cells** (0.25° grid, lat ≤ −85°, pass_count ≥ 3 quality gate)
- C_baseline = 12.9864 counts (AVG(setn_total) over 71,327 Silver rows, computed live)
- NSI = MAX(0, (C_baseline − setn_avg) / C_baseline); 5,563 cells with positive suppression
- RPI = NSI / MAX(mock_slope_deg, 0.1); mock_slope = 2.0° → RPI range 0–0.5
- PSR Haversine cross-reference: 1,771 cells within a PSR boundary
- Top ranked cells cluster near de_Gerlache, Sverdrup, Shackleton, Faustini ✓
- Config: `lunar_ice.south_pole.config_gold_params` (Delta table, no hardcoded values)

### Gold — `lunar_ice.south_pole.gold_v4_shackleton_precision` ✅ v0.04 COMPLETE
- **11,734 cells** (0.25° grid, CSETN1 signal, lat ≤ −85°, pass_count ≥ 3 quality gate)
- C_baseline_csetn = 2.9902 counts (AVG(csetn1_total) over 71,327 Silver rows, computed live)
- NSI_csetn = MAX(0, (C_baseline_csetn − csetn1_avg) / C_baseline_csetn); 5,478 cells positive
- RPI_real = NSI_csetn / GREATEST(slope_avg_deg, 0.1); real LOLA slope from silver_lola_slopes
- LOLA source: `ldem_85s_40m_float.img` (polar GDR, 40 m/pixel, 230 MB download)
- LOLA slope range: 1.8°–33.5°, mean 9.9° — physically validated south polar terrain
- max_RPI = 0.3382 · **RPI ≥ 1.5 threshold not achievable at 0.25° cell resolution**
  (min observed cell slope = 1.8°; max RPI = NSI/1.8 = 0.556 — threshold designed for pixel-level slopes)
- Top ranked cells: de_Gerlache, Faustini, Shackleton (highest NSI + relatively low slope)
- silver_lola_slopes: 30,192 cells (includes corners to −83°), all joined to LEND grid
- Config: `config_gold_params` key `gold_v0.04` · `created_by_agent` tag set

---

## Strategy: Lazy Binary Download

| Layer | Data source | Size | DBU cost |
|---|---|---|---|
| Bronze | PDS3 index.tab (text, 10.6 MB) | ~1 MB Delta | <1 DBU |
| Silver pilot | 3 × RSCI .DAT (binary, ~51 MB each) | ~7k rows Delta | ~1 DBU |
| Silver month | 30 × RSCI .DAT | ~70k rows Delta | ~3 DBU |
| Gold | Spark aggregation on Silver | ~500 grid cells | ~1 DBU |

Binary files downloaded only for Silver-selected products (nominal mission, RSCI type).
Serial download with 0.1s jitter protects USGS government archive from overload.
LOLA slope data fetched in Phase 3 only for the lat ≤ −85° grid cells.

---

## Phase Pivot Log

| Date | Event |
|---|---|
| 2026-05-10 | PDS4 Registry dry-run: all 7 filter combos returned 0 products — LEND not in PDS4 |
| 2026-05-10 | Pivot to PDS3 Geosciences Node confirmed working (HTTP 200, index.tab accessible) |
| 2026-05-10 | Bronze ingestion complete: 28,729 science rows in Delta |
| 2026-05-10 | Label scraping plan dropped: `.LBL` files confirmed to have no lat/lon bounding coords |
| 2026-05-10 | FMT spec fetched: 46 columns, key offsets confirmed (lat@43, lon@51, SETN@274) |
| 2026-05-10 | Silver pilot complete: 7,209 south pole readouts from 3 RSCI daily files |
| 2026-05-10 | Silver June 2010 full month: 71,327 rows, 379 orbits, 30 days |
| 2026-05-10 | Gold v0.03 complete: 11,734 grid cells, RPI ranked, PSR cross-referenced |
| 2026-05-10 | Config pattern: Gold params stored in config_gold_params Delta table (no hardcoded values) |
| 2026-05-10 | Gold v0.04 complete: CSETN + LOLA real slope; RPI ≥ 1.5 threshold not met at 0.25° resolution |
| 2026-05-10 | LOLA GDR discovery: ldem_85s_40m in polar/float_img under lro-l-lola-3-rdr-v1 (not lola-4-gdr) |
| 2026-05-10 | Grid resolution confirmed 0.25° (not 0.1°); CSETN footprint ≈ 9 km justifies sub-footprint via 379-pass avg |

---

## Execution Sequence

```
Phase 1 — Bronze (DONE)
  src/smoke_test.md           ✅ catalog + schema confirmed
  src/volume_setup.md         ✅ raw_pds_blobs volume created
  src/dry_run_fetch.md        ✅ PDS3 HTTP + parse verified
  src/bronze_ingestion.md     ✅ 28,729 rows → bronze_lend_metadata

Phase 2 — Silver (PILOT DONE)
  src/path_distillation.md    ✅ 6,015 RSCI paths → rsci_paths.csv
  src/test_label_scraper.py   ✅ Tracer Bullet → proved .LBL has no lat/lon (plan pivot)
  src/june2010_candidates.md  ✅ 3-day pilot list → june2010_3day_candidates.csv
  src/test_binary_parser.py   ✅ Tracer Bullet → single .DAT parsed, lat filter confirmed
  src/silver_transformation.md  ✅ 7,209 rows → silver_lend_targets (pilot)
  src/silver_june2010_full.md   ✅ 71,327 rows → silver_lend_targets (June 2010 full month)
  [ ] Expand silver to full nominal mission (Sept 2009 – Sept 2010) — Phase 4+

Phase 3 — Gold (DONE)
  ✅ src/config_upload.md      → config_gold_params Delta table (no hardcoded values)
  ✅ src/gold_preflight_c_baseline.md → pass distribution + C_baseline confirmed
  ✅ src/gold_rpi_mapping.md   → 11,734 cells, NSI+RPI, PSR Haversine, gold_psr_rpi_rankings

Phase 4 — High-Fidelity (DONE — action_v0.04.md)
  ✅ src/config_upload_v04.md       → config_gold_params key gold_v0.04 appended
  ✅ src/lola_silver_ingestion.md   → 30,192 slope cells → silver_lola_slopes
       (LOLA GDR ldem_85s_40m_float.img, 230 MB, polar/float_img on Geosciences Node)
  ✅ src/gold_high_res_mapping.md   → 11,734 cells, CSETN NSI + real slope → gold_v4_shackleton_precision
  KEY FINDING: RPI ≥ 1.5 unachievable at 0.25° (min slope 1.8°); threshold meaningful only at pixel-level
  [ ] Databricks App          → south pole ice concentration map, ranked PSR list
  [ ] Chang'e-7 validation    → cross-reference after July 2026 dataset release
  [ ] Phase 5: pixel-level slope from PGDA 5m/pix for Shackleton to reach RPI ≥ 1.5
```

---

## Compute & Resource Constraints

| Constraint | Impact |
|---|---|
| Max 5 concurrent job tasks | Serial download loop; no Spark UDF parallelism for HTTP |
| One 2X-Small SQL warehouse | All Delta writes via Spark Serverless Connect; batch 5k rows |
| One active Lakeflow pipeline | Reserved for Silver→Gold join in Phase 3 |
| No GPU / batch inference | RPI is deterministic formula; no ML needed |
| One Vector Search unit | Not used |

---

## Open Risks

| Risk | Mitigation |
|---|---|
| USGS server outage during multi-day Silver run | Checkpoint: save progress CSV per file; resume from last completed |
| SETN count normalization (C_eq baseline) | Must confirm Release 65 equatorial mean from actual data before Gold |
| LOLA DEM availability for lat ≤ −85° | Confirm PDS3 LOLA gridded product accessible before Phase 3 |
| Chang'e-7 dataset format unknown | Monitor CNSA/ESA data release channels; adapt parser when available |
