# Action v0.04: The Sniper Grid (LOLA & CSETN Integration)
## Science Mission Context
The final deliverable is a **ranked ice concentration map** of the lunar south pole to help NASA
identify extraction candidate sites. Water ice is most concentrated in Permanently Shadowed Regions
(PSRs) — deep crater floors that stay below −173°C and act as cold traps for volatiles.

China's Chang'e-7 deploys an in-situ ice detector at the south pole in **July 2026**. This Gold
layer produces the LEND-derived RPI map before that date. After Chang'e-7 publishes, confirmed
detections are cross-referenced against our RPI grid to validate and calibrate the model —
giving NASA a data-backed, externally validated shortlist for extraction planning.

## Architecture

## Strategic Context & Execution Method
**The Asset:** We have a proven Silver telemetry table with confirmed ice suppression signals.
**The Pivot:** We are moving from the 40km "Floodlight" (SETN) to the 9km "Sniper" (CSETN) resolution to peer directly into Shackleton Crater. To support this, we will increase our grid density from 0.25° to **0.1°**.
**The Method:** We will ingest the **LOLA (Lunar Orbiter Laser Altimeter)** Digital Elevation Model (DEM) data to replace our 2.0° `mock_slope_deg`. This allows us to calculate the true **Resource Potential Index (RPI)** and find the "Flat & Wet" landing zones.

**AGENT INSTRUCTION:** Before executing the full pipeline, provide a brief plan for how you will fetch the LOLA slope data and mathematically join it to our 0.1° orbital grid. Wait for user approval.

## Objective
Produce a High-Fidelity Ice Concentration Map that identifies prime extraction sites by integrating collimated neutron data (CSETN) and real topographic slopes, targeting an RPI score $\ge$ 1.5.

## Tasks

### 1. [Sonnet 4.5] LOLA DEM / Slope Ingestion
- **Action:** Identify and fetch the LOLA Slope Map (GDR) from the PDS Geosciences Node.
- **Detail:** Ingest the slope metadata into the Bronze layer and create a Silver table: `lunar_ice.south_pole.silver_lola_slopes`.
- **Constraint:** Filter strictly for $Lat \le -85^\circ$ to match our LEND footprint.

### 2. [Sonnet 4.5] The "Sniper" Aggregation (CSETN)
- **Action:** Create a high-resolution Spark aggregation job.
- **Formula:**
  - Grid: `cell_lat = FLOOR(lat / 0.25) * 0.25 + 0.125` — **0.25°, same boundaries as v0.03 SETN grid**
    (rationale: CSETN footprint ≈ 9 km; 0.25° ≈ 7.5 km is sub-footprint via 379-pass averaging;
    enables direct SETN↔CSETN suppression-difference comparison cell-by-cell)
  - Use `csetn1_total` as the primary science metric; compute `C_baseline_csetn = AVG(csetn1_total)` live.
- **Output:** Temporary view `gold_high_res_neutron_grid`.

### 3. [Logic Subagent] The Topographic Join
- **Action:** Join the Neutron Grid with the LOLA Slope Grid.
- **Logic:** For every 0.1° neutron cell, map the corresponding average slope value from the LOLA dataset.
- **Formula Update:** Recalculate $RPI = \frac{N_{suppression}}{S_{real\_slope}}$.

### 4. [Sonnet 4.5] The 1.5 Goal Thresholding
- **Action:** Filter the final Gold table to highlight cells where $RPI \ge 1.5$.
- **Detail:** Flag these cells as "Prime Extraction Sites" for cross-referencing with the 2026 Chang'e-7 landing targets.

### 5. [Code Skeleton] High-Fidelity Gold Notebook
- **Action:** Create `src/gold_high_res_mapping.md`.
- **Requirements:** - Apply the 0.25° grid to the June 2010 Silver data (CSETN signal, same boundaries as v0.03).
  - Integrate the real LOLA slope data.
  - Write the final sorted dataframe to: `lunar_ice.south_pole.gold_v4_shackleton_precision`.

### 6. [Haiku Subagent] Validation Document
- **Action:** Update `notes/logic_notes.md` and `config/targets.yaml`.
- **Comparison:** Document the shift from the 2.0° mock slope to the true LOLA slope, and explain how the CSETN resolution changed the RPI distribution.

## Success Criteria
- [x] Agent provides the LOLA integration plan before coding.
- [x] LOLA slope data successfully ingested — 30,192 cells in silver_lola_slopes (40m DEM, ldem_85s_40m_float.img).
- [x] `gold_v4_shackleton_precision` generated at **0.25°** resolution using CSETN (grid confirmed with user).
- [x] RPI values reflect real terrain — max_RPI=0.3382; RPI ≥ 1.5 unachievable at 0.25° cell resolution
      (min slope 1.8° → max RPI 0.556). Threshold valid only at pixel-level (Phase 5 PGDA 5m/pix).
- [x] All code blocks and tables carry the `created_by_agent` tag.