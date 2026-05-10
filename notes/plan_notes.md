# Plan Notes — Lunar Ice Explorer Phase 1
<!-- created_by_agent -->

## Objective
Build a reproducible Bronze ingestion pipeline for LRO LEND neutron suppression data
targeting the Shackleton Crater region (~89.9°S), using a **Metadata-First** strategy
to stay within Databricks Free Tier compute and storage limits.

---

## Phase 1 Scope: Shackleton Baseline

| Dimension | Value |
|---|---|
| Target | Shackleton Crater |
| Center | Lat -89.9°, Lon 0.0° |
| Search radius | 20 km |
| Primary dataset | LRO LEND — EDR + CHK products, PDS3 Release 65 (lro-l-lend-2-edr-v1) |
| Secondary dataset | LOLA slope map (deferred to Phase 2) |
| RPI threshold (baseline) | 1.5 |

---

## Strategy: Metadata-First Ingestion

### Problem
The full LEND binary archive (34,751 products, PDS3, since 2009) is tens of GB.
Downloading it to find relevant data would exhaust Free Tier DBU quota before any
science is done.

### Solution — Index-First, Binary-Last
1. **Stream the PDS3 index file** (`index.tab`, ~10.6 MB) directly from the
   Geosciences Node — no binary data touched.
2. **Ingest all 34,751 index rows** into `bronze_lend_metadata`. Geographic columns
   are absent from the EDR index; since LRO is in a polar orbit, every pass covers
   the south pole region, so all rows are candidates.
3. **Silver layer parses individual `.lbl` files** to extract bounding coordinates
   and filter to the 20 km Shackleton window — deferred to Phase 2.
4. **Lazy binary fetch** — actual `.dat` files pulled into `raw_pds_blobs` volume
   only for Silver-selected products.

> **Why no PDS4 Registry / pds.peppi**: Dry-run (2026-05-10) confirmed LEND data
> is not in the PDS4 Registry. All 7 instrument/host filter combinations returned
> zero products. PDS3 direct access is the only viable path.

---

## PDS3 Direct Access

```python
import io, requests

LEND_BASE = "https://pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/lrolen_0xxx"
INDEX_TAB = f"{LEND_BASE}/index/index.tab"

tab = requests.get(INDEX_TAB, timeout=60, stream=True)
for raw in io.TextIOWrapper(tab.raw, encoding="latin-1"):
    row = raw.rstrip("\r\n")
    # extract fixed-width columns per COLS dict (14 fields, ROW_BYTES=306)
```

Confirmed index schema (34,751 rows, ROW_BYTES=306):
- `PATH_NAME`, `FILE_NAME` — relative path to product .lbl file
- `PRODUCT_TYPE` — LEND_EDR_SCI, LEND_RDR_CHK, LEND_RDR_RSCI, LEND_RDR_DLD/DLX, LEND_EDR_HK
- `RELEASE_ID` — 0011 (bulk), 0025, 0026 (incremental)
- `START_TIME`, `STOP_TIME` — UTC orbit window
- No lat/lon — geographic filter deferred to Silver

---

## Medallion Layer Plan

### Bronze — `lunar_ice.south_pole.bronze_lend_metadata`
- One row per index.tab row (all 34,751 products)
- Columns: `volume_id`, `path_name`, `file_name`, `product_id`, `product_type`,
  `release_id`, `mission_phase`, `start_dt`, `stop_dt`, `ingested_at`, `created_by_agent`
- Partition: `year` (derived from `start_dt`)
- No geographic filter at Bronze; Silver adds bbox from per-product `.lbl` parsing

### Silver — `lunar_ice.south_pole.silver_lend_suppression` (Phase 2)
- Parsed neutron suppression values per orbit-grid cell
- Normalized against LEND Release 65 equatorial baseline

### Gold — `lunar_ice.south_pole.gold_rpi_shackleton` (Phase 2)
- RPI score per grid cell, joined with LOLA slope
- Ready for visualization in Databricks App

---

## Compute & Resource Constraints

| Constraint | Impact on Plan |
|---|---|
| Max 5 concurrent job tasks | Run ingestion serially; no fan-out parallelism |
| One 2X-Small SQL warehouse | All Delta writes via Spark Serverless Connect only |
| One active pipeline per type | Lakeflow pipeline reserved for Silver→Gold; Bronze uses notebook |
| No GPU / batch inference | All ML deferred; RPI is a deterministic formula |
| One Vector Search unit | Not used in Phase 1 |

---

## Execution Sequence (Phase 1 Notebooks)

```
1. src/smoke_test.md         ← Databricks connectivity (DONE)
2. src/volume_setup.md       ← create raw_pds_blobs volume (DONE)
3. src/bronze_ingestion.md   ← stream LEND PDS3 index.tab → bronze_lend_metadata
4. src/dry_run_fetch.md      ← PDS3 connectivity & parse test (DONE — PASS)
```

All notebooks run via:
```bash
PATH="/Users/gwu/.agent_notebook_py312/bin:$PATH" \
  "$AGENT_NB_RUN" <notebook.md> \
  --profile zwapp@protonmail.com \
  --cluster SERVERLESS \
  --format md \
  --output-dir /Users/gwu/Documents/Lunar_ice/.runs/<name>
```

---

## Open Questions / Risks

| Item | Risk | Mitigation |
|---|---|---|
| PDS3 Geosciences Node outage | index.tab fetch fails | Retry with exponential back-off; cache last-good index in raw_pds_blobs volume |
| index.tab schema drift | New release changes column positions | Pin COLS dict to lbl hash; alert if ROW_BYTES changes |
| No lat/lon in EDR index | Cannot filter Shackleton at Bronze | Accepted; Silver fetches per-product .lbl for bbox (10–50 KB each) |
| 34,751 rows × Delta write | Large write on 2X-Small serverless | Use `spark.createDataFrame` in batches of 5,000; partition by year |
