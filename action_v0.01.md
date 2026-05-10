# Action v0.01: Project Initialization & Shackleton Baseline

## Objective
Initialize the local and workspace environment for the Lunar Ice Explorer. Establish the foundational configuration and ingestion logic using the "Metadata-First" strategy to ensure compute efficiency on the Databricks Free Tier.

## Source Data / Scope
- **Target:** Shackleton Crater Region
- **Bounding Box:** Center (Lat: -89.9, Lon: 0.0), Search Radius: 20km.
- **Primary Dataset:** LRO LEND (Lunar Exploration Neutron Detector) - PDS4 Observational Products.
- **Secondary Dataset:** LOLA (Lunar Orbiter Laser Altimeter) for Slope Analysis.
- **Initial RPI Threshold:** 1.5 (Resource Potential Index baseline).

## Tasks

### 1. [Haiku Subagent] Workspace Exploration
- **Action:** Use `agent-notebook --profile zwapp@protonmail.com --serverless` to:
    - Confirm the `Lunar_ice` Catalog exists.
    - Create the `south_pole` schema if missing.
    - Check for an existing Volume named `raw_pds_blobs`.
- **Output:** Log findings in `notes/workspace_status.txt`.

### 2. [Sonnet 4.5] Project Planning
- **Action:** Initialize `plan_notes.md`.
- **Focus:** Outline the Phase 1 strategy focusing on PDS4 Registry querying via `pds.peppi`. Detail how to avoid full-dataset downloads by filtering on-orbit before ingestion.

### 3. [Logic Subagent] Mathematical Baseline
- **Action:** Create `notes/logic_notes.md`.
- **Formula Definition:** 
    - Define the $RPI$ (Resource Potential Index) calculation:
    $$RPI = \frac{\text{Neutron Suppression (Normalized)}}{\text{Terrain Slope (Degrees)}}$$
    - Document the normalization constants for LEND Release 65 (March 2026).

### 4. [Physical Setup] Infrastructure & Config
- **Action:**
    - Create directories: `/config`, `/src`, `/notes`.
    - Create `config/targets.yaml` with the Shackleton coordinates and RPI 1.5 threshold.
    - Ensure no hardcoded values exist in code skeletons.

### 5. [Code Skeleton] Bronze Ingestion Notebook
- **Action:** Create `src/bronze_ingestion.md`.
- **Requirements:**
    - Import `pds.peppi` and `pyyaml`.
    - Logic to load `config/targets.yaml`.
    - A placeholder function `fetch_lend_metadata()` that queries the PDS Registry.

### 6. [Haiku Subagent] Post-Action Chronicling
- **Action:** Create `code_study_notes.md`.
- **Detail:** Explain the rationale for the "Metadata-First" approach to preserve the 5-concurrent-job limit and DBU quota on the Free Tier.

## Success Criteria
- [ ] Directory structure verified locally.
- [ ] `Lunar_ice.south_pole` verified in Databricks Unity Catalog.
- [ ] `src/bronze_ingestion.md` executes successfully in Serverless mode (import check only).
- [ ] All code contains the `created_by_agent` tag in metadata.