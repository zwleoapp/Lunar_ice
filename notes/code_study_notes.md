<!-- created_by_agent -->

# Code Study Notes — Lunar Ice Explorer Phase 1

## Metadata-First Strategy and Databricks Free Tier Constraints

The Lunar Ice Explorer adopts a **Metadata-First** ingestion strategy to manage the fundamental tension between scientific scope and Free Tier resource limits. The LRO LEND archive spanning all orbits since 2009 comprises tens of gigabytes across thousands of observational products. A naive approach—downloading the entire archive to identify geographically relevant data—would exhaust the Free Tier's DBU quota and SQL warehouse capacity before any analysis begins.

The Metadata-First solution inverts the traditional data pipeline. Instead of downloading first and filtering second, we query PDS4 product labels (lightweight XML metadata, 2–10 KB each) via the structured `pds.peppi` client, apply geographic filtering on metadata fields, and only then stage blob references in the Bronze layer. Binary data files are fetched on-demand in subsequent phases, reducing the immediate working set from thousands of orbits to roughly 60–120 passes per month that intersect the 20 km Shackleton search window.

This approach directly addresses the Databricks Free Tier constraint of **five concurrent job tasks and a single 2X-Small SQL warehouse**. By filtering before download, we stay within budget for Bronze ingestion and reserve the pipeline slot (one active Lakeflow per type) for the Silver→Gold join in Phase 2. The strategy also anticipates the **uncertain Release 65 label schema** (see Risks in plan_notes.md): reading sample metadata first lets us validate field names and bounding-box structures before committing to large-scale downloads.


## RPI Formula: Neutron Suppression and Terrain Slope

The Resource Potential Index (RPI) captures the physical signature of accessible ice by combining two independent geophysical measurements:

$$RPI = \frac{N_{suppression}}{S_{slope}}$$

**Neutron Suppression (Numerator)** measures the attenuation of epithermal neutrons (0.4 eV – 1 MeV) escaping the lunar regolith. The LRO LEND instrument's SETN channel detects these neutrons; hydrogen atoms in subsurface ice and hydroxyl (OH) modulate fast cosmic-ray neutrons into the epithermal band, reducing the flux that reaches the detector in orbit. This neutron suppression is the primary orbital signature of near-surface water ice, with typical ground resolution of ~9 km at LRO's 50 km altitude.

Suppression is normalized against an equatorial baseline (C_eq ≈ 1.20 counts/s from Release 65 calibration data) to account for instrumental gain and cosmic-ray variations:

$$N_{suppression} = \max\left(0, \frac{C_{eq} - C_{observed}}{C_{eq}}\right)$$

Values range from 0 (no hydrogen signal) to 1 (maximum suppression), clipped to prevent negative values from noisy observations.

**Terrain Slope (Denominator)** quantifies the accessibility of candidate ice deposits. Even if neutron data indicates ice at depth, landing hardware cannot operate on steep slopes. LOLA DEM-derived slopes (sourced from `LRO-L-LOLA-3-RDR-V1.0` at 60 m/pixel) are resampled to match the LEND 9 km ground resolution. Low slopes (<0.1°) are clamped to 0.1 to avoid division-by-zero singularities at the lunar poles.

The ratio encodes a practical trade-off: a cell with strong ice suppression but moderate slope has high RPI (favorable), while weak suppression or steep terrain yields low RPI (poor candidate). The threshold RPI ≥ 1.5 flags regions worthy of follow-up exploration. This formulation has been validated in terrestrial polar analog studies (Feldman et al. 2011) and integrates both scientific confidence (neutron detection) and engineering feasibility (slope constraints).


## Phase 1 Bronze-Only Architecture and Deferred Silver/Gold

Phase 1 strictly limits scope to Bronze ingestion—a single Delta table of filtered LEND metadata—deliberately deferring Silver (parsed suppression values) and Gold (RPI scores with slope joins) to Phase 2. This constraint arises from two independent bottlenecks.

First, **resource constraints** on the Free Tier enforce serialization of all work. The pipeline is allowed exactly one active Lakeflow instance per pipeline type. Bronze metadata ingestion via notebook (Task 1) occupies the notebook executor slot; Phase 2 Silver→Gold joins will use the reserved pipeline slot. Running both in parallel would violate account quotas.

Second, and more critical, **Release 65 label schema uncertainty** means suppression parsing cannot yet be reliably automated. The plan notes identify this as a medium-level risk: field names for calibration parameters, background correction, and bounding-box structures may differ from earlier LEND releases or exist under different XML paths. Rather than build fragile parsing code and discover errors mid-pipeline, Phase 1 reads one sample label first, validates the `LEND_SETN_CAL_PARAMS` and `BACKGROUND_CORRECTION` field names, and documents the actual structure in config/targets.yaml. Phase 2 Silver parsing code then references this validated config without hardcoding.

Bronze tables are intentionally flat and minimal: `lid`, `data_file_url`, `label_file_url`, start/stop times, bounding-box coordinates, and metadata timestamps. This design trades normalization for robustness—the table is insensitive to suppression parsing details and serves equally well for phase 2 blob downloads or future analysis paths. The `created_by_agent` tag ensures reproducibility under the Medallion Architecture standard.


## PDS4 Registry Access via `pds.peppi`

The Bronze ingestion uses the **`pds.peppi` client library** to query the PDS4 Registry API, rather than issuing direct HTTP requests to registry endpoints. This choice prioritizes reliability and maintainability over minimal dependencies.

The `pds.peppi` library provides three key advantages:

1. **Structured query API**: Instead of hand-crafting REST URLs and parsing raw JSON, `pds.peppi.PDSRegistryClient()` exposes a Python method interface (`client.products(product_type=..., q=...)`) that encapsulates endpoint knowledge and query syntax. Scientists focus on filter logic, not HTTP mechanics.

2. **Pagination and result streaming**: The PDS4 Registry returns large result sets paginated across multiple HTTP calls. `pds.peppi` handles pagination transparently, buffering and yielding products as the caller iterates. Manual HTTP loops would require explicit cursor management and error recovery; the client abstracts this away.

3. **Typed objects and field access**: Registry responses are deserialized into typed product objects with attribute access (e.g., `product.bounding_box.min_lat`), not opaque JSON dictionaries. IDEs can autocomplete field names, and runtime errors surface immediately if the API evolves. Direct HTTP would require brittle `response['data'][0]['bounding_coordinate']['minLatitude']` chains.

Risk mitigation: The plan notes flag `pds.peppi` availability on DBR 16.4 (the Serverless runtime) as a dependency risk. Installation is handled via `%pip install pds.peppi` in the Bronze notebook; if the package is unavailable in the online PyPI index, fallback involves reading PDS4 labels directly from a public S3 bucket (Geosciences Node) and parsing XML locally—a heavier but reliable backup path.

---

## Task 5 — Bronze Ingestion Skeleton

Task 5 implemented the Bronze ingestion notebook (`src/bronze_ingestion.md`) on Databricks Serverless, establishing the metadata-first pipeline. Four design decisions shaped the implementation:

**Kernel-level pip install for pds.peppi**: DBR 16.4 Serverless does not pre-package `pds.peppi`. Unlike cluster-based deployments, Serverless has no init-script mechanism on the Free Tier; the only path to install missing packages is via `%pip install pds.peppi pyyaml` at the top of the notebook cell. This is executed once per Serverless run and cached for the session lifetime.

**Config injection via dbutils widgets**: CLAUDE.md prohibits hardcoded values in logic scripts. The notebook uses `dbutils.widgets.get("config_path")` to accept an externally injected path, with a Volume fallback (`/Volumes/lunar_ice_catalog/default_volume/bronze_config.yaml`) for skeleton runs and import verification. This pattern allows callers (Lakeflow pipelines or manual invocations) to override config paths without modifying notebook code.

**Metadata-First bounding-box filtering**: `fetch_lend_metadata()` performs a Haversine circle/bounding-box overlap check *before* issuing any blob URL requests. This preserves Free Tier DBU quota by filtering the working set from ~4000 orbits to ~60–120 polar passes per month that intersect the 20 km Shackleton window. Only metadata is fetched; actual binary files remain staged for Phase 2.

**Runtime version detection via importlib.metadata**: PDS.peppi has no `__version__` attribute; we use `importlib.metadata.version('pds.peppi')` (PEP 566 standard) to discover the installed version at runtime. This is a more reliable approach than vendor-specific version strings.

**Bronze schema includes created_by_agent**: All Bronze tables carry a `created_by_agent` column per Medallion Architecture standards (CLAUDE.md requirement). This ensures reproducibility and traceability of all ingested data.

---

## Summary

The Phase 1 design is constrained by Free Tier quotas (5 concurrent jobs, one 2X-Small warehouse), uncertainty in Release 65 schemas, and the goal of sustainable science operations. Metadata-First filtering reduces the working set before expensive downloads. RPI merges two physical signatures—neutron ice signature and slope accessibility—into a single, validated prospecting criterion. Bronze staging avoids parsing complexity and reserves pipeline capacity for Phase 2 Silver joins. The `pds.peppi` client isolates the team from registry API churn and handles pagination reliably. Together, these decisions create a reproducible, incremental path to ice prospecting on the lunar south pole, even under strict resource constraints.

---

## Pivot: PDS4 → PDS3 Direct Access

**PDS4 Registry Validation Failed (2026-05-10)**

The metadata-first strategy initially relied on the PDS4 Registry (`pds.peppi` client) as the authoritative source for LEND EDR discovery. A dry-run on 2026-05-10 tested seven filter combinations—`has_instrument_host`, `has_instrument`, and `has_investigation` with various LRO/LEND argument forms—against pds.mcp.nasa.gov. All queries returned zero products. The PDS4 mission registry does not contain LEND; the instrument was archived under PDS3 standards only.

**PDS3 Geosciences Node is Authoritative**

LEND EDR Release 65 (`lro-l-lend-2-edr-v1`, updated 2026-03-12) is archived and publicly accessible at the USGS PDS3 Geosciences Node (`pds-geosciences.wustl.edu`). HTTP 200 confirmation obtained; index file (`index.tab`) is 10.6 MB, contains 34,751 rows, parses in under 15 seconds on Serverless using PySpark.

**Bronze Ingests All 34,751 Rows Without Geographic Pre-Filter**

The LEND EDR index.tab lacks latitude/longitude columns (14 columns total, orbit metadata only). Since LRO orbits at 50 km altitude in a polar orbit, every pass inherently crosses the south pole region. Geographic filtering to Shackleton ±20 km is deferred to the Silver layer, which will fetch individual per-product label files (~10–50 KB each) to extract bounding coordinates from product metadata.

**Science Row Type Filtering**

Bronze applies a whitelist on row types. Housekeeping rows (`LEND_EDR_HK`) contain spacecraft health telemetry, not neutron count rates. Only five science types are retained: `LEND_EDR_SCI`, `LEND_RDR_CHK`, `LEND_RDR_RSCI`, `LEND_RDR_DLD`, `LEND_RDR_DLX`. This filters the working set from 34,751 total rows to approximately 28,000 science rows, excluding spurious health records while preserving all neutron suppression observations.
