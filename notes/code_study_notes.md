<!-- created_by_agent -->

# Code Study Notes — Lunar Ice Explorer Phase 1

## Metadata-First Strategy: Rationale

The Lunar Ice Explorer adopts a **Metadata-First** ingestion strategy because the full LEND archive (34,751 products, spanning 2009–2026) is tens of gigabytes in binary format. Downloading it naively would exhaust the Databricks Free Tier DBU quota and SQL warehouse capacity before any science begins.

**The inversion**: Stream the PDS3 index file (`index.tab`, 10.6 MB) first; fetch binary `.dat` files last and only for Silver-selected products. This keeps Phase 1 Bronze ingestion under 1 DBU.

**Five-job concurrency limit (Databricks Free Tier)**: The account supports exactly 5 concurrent job tasks. A full binary download would require fan-out parallelism across hundreds of files — immediately saturating the limit and triggering queue delays. The Metadata-First approach is fully serial: one streaming HTTP connection, one Spark DataFrame write loop in batches of 5,000. It never touches the concurrency ceiling.

**One 2X-Small SQL warehouse**: All Delta writes go through Spark Serverless Connect. Batching 5,000-row chunks per write call prevents memory pressure on the 2X-Small node, while staying within a single warehouse session.

**One active Lakeflow pipeline slot**: Reserving the pipeline slot for the Silver→Gold join (Phase 2) means Bronze must run as a plain notebook. This is fine: index.tab is a single file, no pipeline complexity needed.


## PDS4 → PDS3 Pivot (2026-05-10)

The original plan used the PDS4 Registry (`pds.peppi` client) for LEND product discovery. A dry-run on 2026-05-10 tested seven filter combinations against pds.mcp.nasa.gov — all returned zero products. LEND was never migrated to PDS4; it lives exclusively under PDS3 on the USGS Geosciences Node.

**PDS3 Geosciences Node** (`pds-geosciences.wustl.edu`):
- Archive: `lro-l-lend-2-edr-v1/lrolen_0xxx/` — Release 65, updated 2026-03-12
- `index/index.tab`: 10.6 MB, 34,751 rows, 14 fixed-width columns, ROW_BYTES=306
- **No lat/lon columns in the index** — LRO's polar orbit means every pass covers the south pole. Geographic filter to Shackleton ±20 km is deferred to Silver, which parses per-product `.lbl` files (~10–50 KB each) for bounding coordinates.

This discovery does not change the Metadata-First strategy — it only changes the metadata source from PDS4 XML to PDS3 fixed-width index.


## Bronze Ingestion Design Decisions

**Science row whitelist**: `LEND_EDR_HK` rows contain spacecraft health telemetry, not neutron counts. Bronze retains only five science types: `LEND_EDR_SCI`, `LEND_RDR_CHK`, `LEND_RDR_RSCI`, `LEND_RDR_DLD`, `LEND_RDR_DLX`. This reduces 34,751 total rows to ~28,000 science rows.

**Fixed-width column parsing**: PDS3 `index.tab` uses fixed-width fields defined in `index.lbl` (START_BYTE is 1-indexed). Column offsets are stored in a `COLS` dict with `(start, length)` tuples. The `col()` helper slices, strips whitespace and quotes. This is direct and requires no external parser library — `requests` is already in the base DBR runtime.

**No `%pip install` in production notebook**: `requests` is pre-installed in DBR 16.4 Serverless. Adding `%pip install` triggers a kernel restart, losing all variable state between cells. The notebook uses only stdlib + pre-installed packages.

**Config via dbutils widgets**: CLAUDE.md prohibits hardcoded values. The notebook accepts `config_path` as a widget parameter; `targets.yaml` supplies catalog, schema, and volume names. Defaults fall back to a safe literal dict for smoke-test runs.

**Delta partition by `release_id`**: Release 65 has three distinct release IDs (`0011` bulk, `0025`, `0026` incremental). Partitioning by `release_id` enables efficient Silver-layer queries that target specific release windows without full-table scans.


## Silver Layer — Binary Parse Architecture (Action v0.02, 2026-05-10)

### Why Label Scraping Was Dropped
The original Silver plan assumed `.LBL` files carry `MINIMUM_LATITUDE`/`MAXIMUM_LATITUDE` bounding
coordinates. The Tracer Bullet test (2026-05-10) disproved this: LEND_RDR_RSCI `.LBL` files are
format descriptors only — 46-column schemas with no spatial content. Spatial data is exclusively
inside binary `.DAT` files. Label scraping was dropped entirely before any production code was written.
Cost of discovery: one HTTP request, zero DBU.

### Option A + C Architecture
- **Option A (Phase Filter):** Bronze table queried for `mission_phase NOT IN ('CRUISE', 'COMMISSIONING')`.
  Drops pre-orbital and early-commissioning data where the spacecraft had not yet settled into its
  nominal polar science orbit. Costs zero extra compute — a SQL WHERE clause on the existing Delta table.
- **Option C (Binary Parse):** Download `.DAT` files, parse every 594-byte row with Python `struct`,
  apply lat ≤ −85° filter on actual detector readings. The filter yields ~2.8% of rows per file
  (orbital geometry: ~3 min of each 113-min orbit is spent below −85° latitude).

### Polite Serial Download Strategy
6,015 RSCI paths exist in the Bronze table. Parallel Spark UDF download would send hundreds of
simultaneous HTTP connections to `pds-geosciences.wustl.edu` — a US government academic archive
with no published rate limit. Risk: IP ban that blocks all future LEND access.
Decision: serial Python loop with `time.sleep(0.1)` between files. At ~20 MB/s download speed,
3 files took 47 seconds total. For a full month (~30 files) this is ~10 minutes — acceptable for
a one-time Silver build. Government servers are treated as shared infrastructure, not private APIs.

### FMT Spec Discovery
`LEND_RDR_RSCI.FMT` (fetched from `LABEL/` directory, 2026-05-10) defines all 46 binary columns.
Key findings that shaped the parser:
- `SETN_SPECTRUM` is a 16-bin array (not a scalar) — must sum all 16 bins for total count
- `CSETN1–4` collimated sensors at 9 km ground resolution (vs 40 km for uncollimated SETN) —
  preferred for Shackleton-scale (~21 km) PSR targeting in Gold layer
- `POINTING` and `INTERSECTING` quality flags — rows where pointing data is unavailable have
  zeroed position vectors and must be excluded from the spatial grid
- The FMT has a typo: column 7 is spelled `LONGITIUDE` — noted in `lend_rsci_fmt.yaml`
- All floats are big-endian IEEE 754 double (MSB) — Python `struct` format `">d"`

### Silver Results (2026-05-10)
**Pilot (3 days):** 259,200 raw rows → 7,209 passing (2.8%), 153.9 MB, 47s, zero USGS errors

**Full June 2010 (30 days):** Same serial loop extended to full month:
- 2,592,000 raw rows → 71,327 passing (2.75%)
- 1.54 GB downloaded across 30 files, 480s total (~8 min), zero errors or gaps
- Day-to-day passing count consistent (2,060–2,480 range); June 25 slight dip, not anomalous
- 379 distinct orbits → each 0.25° grid cell averages 10–13 passes (above confidence threshold)
- SETN mean 12.99 counts, stddev 4.12 — meaningful signal variance, not noise-dominated
- Lat minimum −89.9952° — spacecraft flew within 0.005° of geographic south pole
- Table: `lunar_ice.south_pole.silver_lend_targets` — Delta, partitioned by mission_phase
- All 5 neutron channels stored: SETN (40 km) + CSETN1–4 (9 km collimated)

### Chang'e-7 Validation Path
China's Chang'e-7 mission (July 2026) deploys an in-situ ice detector at the south pole.
Once the dataset is published, confirmed ice detection sites can be cross-referenced against
the Gold RPI grid. PSR cells with RPI ≥ 1.5 that align with Chang'e-7 detections validate
the LEND-derived model. This gives NASA a data-backed shortlist for extraction site planning.

---

## Gold Layer — Design Decisions (Action v0.03, 2026-05-10)

### Config-Driven Pipeline (no hardcoded values)
All Gold pipeline parameters are stored in `lunar_ice.south_pole.config_gold_params` (Delta table,
Unity Catalog). The notebook reads JSON config at runtime:
- `mock_slope_deg`, `slope_clamp_deg`, `min_pass_count`, `grid_deg`, `moon_radius_km`
- Full PSR inventory with coordinates and diameters
- Table names for Silver and Gold layers
Using a Delta table instead of YAML files avoids Databricks Serverless filesystem permission
issues (`dbutils` and `/Volumes` FUSE are unavailable in the Databricks Connect kernel).
Config changes require re-running `src/config_upload.md` only.

### C_baseline — Computed Live from Silver
`C_BASELINE = AVG(setn_total)` is computed at notebook runtime from the Silver table, not
hardcoded. This guarantees the baseline is always in sync with the actual ingested data.
Confirmed value for June 2010: **12.9864 counts** (stddev 4.1197, 71,327 rows).

### FLOOR Binning (confirmed method)
`cell_lat = FLOOR(lat / GRID_DEG) * GRID_DEG + GRID_DEG/2` — predictable 0.25° cell centres,
no ambiguity at grid edges. GRID_DEG read from config table. Ready for 0.1° upgrade in v0.04.

### Pass-Count Quality Gate
Pre-flight confirmed: 23,933 distinct cells; 12,199 dropped (pass < 3), 11,734 surviving (49%).
Distribution is smooth decreasing (not bimodal): fringe cells (near −85°) get 1–2 passes;
polar cells get up to 39 passes. Gate at 3 is correct: n≥3 gives ~18% SE, detectable against
10–20% PSR suppression signal.

### mock_slope_deg = 2.0° (config-sourced)
PSR floors are typically 1–3°. Value of 5° would deflate RPI for the exact target cells.
With mock_slope = 2.0, max RPI = NSI_max / 2.0 = 0.5. The 1.5 threshold in targets.yaml
applies only when LOLA real slopes replace the mock in v0.04.

### Haversine in Python Driver (not Spark UDF)
11,734 cells collected to driver after aggregation — well within memory. Python loop over
PSRS list (7 PSRs) computes nearest PSR distance and within_psr flag. More debuggable than
a registered Spark UDF, and fast enough at this scale (< 1s).

---

## Gold Layer v0.04 — LOLA + CSETN Design Decisions (Action v0.04, 2026-05-10)

### LOLA Archive Discovery (tracer bullet)
The LOLA GDR is NOT at `lro-l-lola-4-gdr-v1.0` (404). It lives inside the RDR volume:
`lro-l-lola-3-rdr-v1/lrolol_1xxx/data/lola_gdr/polar/float_img/` on pds-geosciences.wustl.edu.
The PGDA GSFC site (pgda.gsfc.nasa.gov) has 5m/pixel site-specific DEMs (GeoTIFF), not parseable
without GDAL. The polar/float_img directory has `ldem_*_float.img` products as float32.

### ldem_85s_40m_float.img — Format Quirks
- `MAP_SCALE = 40` in .lbl is in **metres per pixel** (not km). My code divides by 1000 to get km/pix.
- DEM values are in **km** (not metres) — `min=-6, max=7` = -6 km to +7 km relative to reference sphere.
  Slope gradient must use `chunk * 1000` to convert km→m before dividing by PIXEL_M (metres).
- `SAMPLE_TYPE = PC_REAL` = IEEE 754 float32 little-endian. `MISSING_CONSTANT` absent — file is gap-free.
- Projection: south polar stereographic, row 0 = south pole perimeter, center at pole.
  Inverse formula: `lat = -90 + 2 × atan(rho_km / (2 × 1737.4))`, `lon = atan2(x, y)`.

### RPI Threshold Finding
At 0.25° cell resolution, the minimum LOLA slope observed is 1.8° (cell-averaged).
max_RPI = NSI_max / slope_min = 1.0 / 1.8 = 0.556 — well below the 1.5 goal.
The 1.5 threshold is designed for pixel-level slopes (PGDA 5m/pix); it is unachievable
at LEND grid resolution. Phase 5 should use PGDA Shackleton 5m DEM for the final threshold map.

### v0.04 Grid Resolution Decision: 0.25° (not 0.1°)
The original v0.04 spec called for 0.1° to "peer into Shackleton." Analysis against CSETN physics
showed 0.1° is over-resolved: at −89°S a 0.1° longitude cell is only 53 m — 170× smaller than
the 9 km CSETN footprint. Adjacent lon-cells share the same measurement, inflating apparent
cell count without adding information. 0.25° matches the v0.03 SETN grid, enabling a direct
suppression-difference map (SETN vs CSETN), and 379-pass averaging defends sub-footprint
precision in latitude. The real v0.04 upgrades are CSETN signal + LOLA real slope, not density.

### Gold Results (2026-05-10)
- 11,734 cells written to `gold_psr_rpi_rankings` ordered by RPI DESC
- 5,563 cells with positive NSI (47%); max NSI = 1.0; avg RPI = 0.033
- 1,771 cells within a PSR boundary (15%)
- Within-PSR avg RPI 0.0336 > outside 0.0329 ✓
- Top cells cluster near de_Gerlache, Sverdrup, Shackleton, Faustini — correct PSRs

---

## Phase 4 Completion — CSV Export + GitHub Publication (2026-05-10)

### CSV Export Design (`src/export_gold_csv.md`)
Gold v0.04 exported to local CSV via `spark.sql().toPandas()` + `pandas.to_csv()`.

**Key path decision:** `OUT_PATH` must be a **local filesystem path**, not a Unity Catalog
Volume path. The agent-notebook CLI runs in a local Python 3.12 venv via Databricks Connect —
`/Volumes/...` FUSE mounts do not exist locally. `spark.sql()` reads remote Delta tables fine
(Databricks Connect tunnels the query); only the final `to_csv()` write is local.

Correct path: `/Users/gwu/Documents/Lunar_ice/data/gold_v4_psr_rankings.csv`

### Annotation Columns Added
Two derived columns added at export time (not stored in Gold Delta table — kept lean):
- `priority_tier`: `Prime` (NSI ≥ 0.6 AND slope ≤ 6°) / `Watch` (NSI ≥ 0.3) / `Background`
- `annotation`: human-readable note for notable cells (within-PSR confirmed, strong suppression)

### Export Results (2026-05-10)
- 11,734 rows · 12 columns · 890 KB
- Prime cells: high-suppression + accessible terrain (de Gerlache, Faustini)
- Within-PSR annotated cells confirmed for Faustini interior

### GitHub Publication
Repo: `github.com/zwleoapp/Lunar_ice` (already initialised from Phase 1).
Published in one commit: README.md, data/gold_v4_psr_rankings.csv, all src/ notebooks,
config/, notes/, action_v0.01–v0.04.md.

**README.md design rationale:** Concise GitHub landing page (~100 lines) referencing
`notes/south_pole_targeting_report.md` for full science detail. Contains PSR rankings table,
column schema, medallion architecture ASCII diagram, reproducibility notebook order, and
RPI ceiling explanation — enough for a NASA reader to understand findings without running code.

---

## RPI Formula

$$RPI = \frac{N_{suppression}}{S_{slope}}$$

**Neutron Suppression (numerator)**: LEND SETN channel measures epithermal neutron flux (0.4 eV – 1 MeV). Hydrogen from subsurface water ice reduces the flux. Normalized against the Release 65 equatorial baseline (C_eq ≈ 1.20 counts/s):

$$N_{suppression} = \max\left(0, \frac{C_{eq} - C_{observed}}{C_{eq}}\right)$$

**Terrain Slope (denominator)**: LOLA DEM-derived slope in degrees at 60 m/pixel, resampled to LEND 9 km ground resolution. Clamped to 0.1° minimum to avoid division by zero at the poles.

RPI ≥ 1.5 flags cells for follow-up. High suppression + low slope = accessible ice candidate.


## Phase 1 Execution Sequence

| Step | Notebook | Status |
|------|----------|--------|
| 1 | `src/smoke_test.md` | DONE — lunar_ice catalog confirmed, south_pole schema created |
| 2 | `src/volume_setup.md` | DONE — raw_pds_blobs volume at `/Volumes/lunar_ice/south_pole/raw_pds_blobs` |
| 3 | `src/dry_run_fetch.md` | DONE — HTTP 200, 200 rows parsed in 13.7s, all 14 columns clean |
| 4 | `src/bronze_ingestion.md` | READY — streams full 34,751-row index → bronze_lend_metadata |

Silver and Gold layers are deferred to Phase 2.
