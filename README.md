# Lunar Ice Explorer — South Pole Pathfinder

**AI-assisted planetary science pipeline** · Claude (Anthropic) + Databricks Serverless + NASA PDS telemetry

> Three Permanently Shadowed Regions — de Gerlache, Faustini, and Shackleton — show statistically significant epithermal neutron suppression consistent with near-surface hydrogen (water ice). Total compute cost: **< 10 DBU** on a Databricks Free Tier account.

---

## Quick Start — Science Data

The primary output is [`data/gold_v4_psr_rankings.csv`](data/gold_v4_psr_rankings.csv) — **11,734 cells** covering the lunar south pole (lat ≤ −85°), each with CSETN neutron suppression, LOLA terrain slope, and Resource Potential Index (RPI).

| Column | Description |
|---|---|
| `cell_lat`, `cell_lon` | 0.25° grid cell centre (degrees) |
| `nsi` | Neutron Suppression Index — `max(0, (C_baseline − cell_avg) / C_baseline)` |
| `slope_deg` | LOLA DEM cell-average slope (degrees) |
| `rpi` | Resource Potential Index — `nsi / slope_deg` |
| `nearest_psr_name` | Closest named PSR |
| `within_psr` | True if cell centre is within PSR boundary (Haversine) |
| `priority_tier` | `Prime` (NSI ≥ 0.6 & slope ≤ 6°) / `Watch` / `Background` |
| `annotation` | Human-readable note for top candidates |

---

## Architecture — Medallion Pipeline

```
NASA PDS Geosciences Node
  │  LEND Release 65 — 230 MB binary telemetry (PDS3)
  │  LOLA GDR ldem_85s_40m_float.img — 230 MB float32 DEM
  │
  ▼ BRONZE  lunar_ice.south_pole.bronze_lend_metadata
  │  28,729 rows · PDS3 index, orbit metadata
  │
  ▼ SILVER  lunar_ice.south_pole.silver_lend_targets
  │  71,327 detector readouts · 379 LRO orbits · June 2010
  │
  ├─▶ SILVER  lunar_ice.south_pole.silver_lola_slopes
  │    30,192 cells · LOLA DEM finite-difference slopes
  │
  ▼ GOLD v0.03  lunar_ice.south_pole.gold_psr_rpi_rankings
  │  SETN (~40 km footprint) + mock slope — regional map
  │
  ▼ GOLD v0.04  lunar_ice.south_pole.gold_v4_shackleton_precision
     CSETN (~9 km footprint) + real LOLA slope — PSR-targeting map
     11,734 cells · max RPI = 0.338 · C_baseline_csetn = 2.9902
```

All notebooks are in [`src/`](src/) as executable Markdown (Databricks format). Config in [`config/`](config/). Notes and scientific rationale in [`notes/`](notes/).

---

## Key Findings

### PSR Rankings (three axes kept separate)

| PSR | Ice Signal (NSI) | Terrain (slope) | Confidence (passes) | Priority |
|---|---|---|---|---|
| **de Gerlache** (−88.3°, 272°E · ⌀32 km) | ★★★ up to 1.0 | ★★ 3–5° avg | ★★★ multiple 3–5 pass cells | **1** |
| **Faustini** (−87.2°, 83°E · ⌀43 km) | ★★★ 0.55–1.0; within-PSR confirmed | ★★★ flattest floor | ★★★ PSR interior cell confirmed | **1** |
| **Shackleton** (−89.7°, 130°E · ⌀21 km) | ★★ 0.47–0.89; likely underestimated | ★ rim-dominated 20–30° | ★★★ 379 orbits overhead | **watch** |
| Sverdrup (−88°, 153°E) | ★★ NSI up to 1.0 | ★★ moderate | ★★ 3+ passes | honourable mention |

**Faustini** is the most geometrically accessible target for a lander or rover — large flat floor, confirmed within-PSR cell, strong suppression.

**Shackleton** NSI is likely systematically underestimated because the 9 km CSETN footprint straddles floor and rim simultaneously. Should be re-evaluated at pixel-level resolution (Phase 5).

### The RPI Ceiling — A Physical Constraint

```
RPI_max = NSI_max / slope_min = 1.0 / 1.80° = 0.556
```

The designed threshold of RPI ≥ 1.5 is unachievable at 0.25° grid resolution. The minimum observable cell-average slope is 1.80° because each 0.25° cell (~7.5 km) integrates both the flat PSR floor and the adjacent crater wall. This is not a calibration error — it quantifies a hard spatial resolution limit of orbital neutron sensing that is not explicitly stated in the LEND instrument papers.

---

## Data Scope

- **Date range:** June 2010 (30 days) — 8% of the nominal LRO mission
- **Orbits:** 379 · **Silver rows:** 71,327 · **Lat filter:** ≤ −85°
- **Full mission scale (Phase 5):** Sept 2009–Sept 2010, ~4,400 orbits, ~875k rows

---

## Reproducing the Pipeline

All source data is openly accessible on the NASA PDS Geosciences Node (no account required):

- **LEND telemetry:** `pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/`
- **LOLA DEM:** `pds-geosciences.wustl.edu/lro/lro-l-lola-3-rdr-v1/lrolol_1xxx/data/lola_gdr/polar/float_img/ldem_85s_40m_float.img`

Run notebooks in order:
```
src/bronze_ingestion.md          → bronze_lend_metadata
src/silver_june2010_full.md      → silver_lend_targets
src/lola_silver_ingestion.md     → silver_lola_slopes
src/config_upload_v04.md         → config_gold_params (Gold v0.04 params)
src/gold_high_res_mapping.md     → gold_v4_shackleton_precision
src/export_gold_csv.md           → data/gold_v4_psr_rankings.csv
```

Requires: Databricks account with Unity Catalog, catalog `lunar_ice`, schema `south_pole`, volume `raw_pds_blobs`.

---

## Engineering Note

The pipeline — ingesting 230 MB of NASA binary telemetry, parsing undocumented PDS3 binary formats, computing neutron suppression maps, joining LOLA terrain slopes, and ranking PSR extraction candidates — was designed, executed, debugged, and documented autonomously by **Claude Sonnet 4.6** (Anthropic) operating against Databricks Serverless compute. Human oversight provided scientific direction, data scope decisions, and quality gate approvals.

See [`notes/south_pole_targeting_report.md`](notes/south_pole_targeting_report.md) for the full Pathfinder Summary Report.

---

*Source data: NASA PDS (public domain). Pipeline and analysis: MIT License.*  
*Contact for collaboration or Phase 5 scaling: zwapp@protonmail.com*
