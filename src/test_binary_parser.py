# created_by_agent
# Tracer Bullet: download one LEND_RDR_RSCI .DAT file, parse binary rows,
# filter to lat <= -85.0, print south pole passes with PSR proximity.
# Run locally: python3 src/test_binary_parser.py

import csv, struct, tempfile, pathlib, requests, time

LEND_BASE  = "https://pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/lrolen_0xxx"
CSV_PATH   = pathlib.Path(__file__).parent.parent / "config" / "june2010_3day_candidates.csv"
ROW_BYTES  = 594
LAT_GATE   = -85.0

# PSR reference points (lat, lon, name) — lunar fixed coordinates, lon 0-360
PSRS = [
    (-89.67, 129.47, "Shackleton"),
    (-88.33, 272.01, "de Gerlache"),
    (-87.47, 355.33, "Haworth"),
    (-87.20,  83.25, "Faustini"),
    (-88.00, 152.50, "Sverdrup"),
    (-85.18,  53.53, "Nobile"),
    (-84.90, 316.00, "Cabeus"),
]

def nearest_psr(lat, lon):
    best, best_d = "—", 999
    for plat, plon, name in PSRS:
        d = ((lat - plat)**2 + (lon - plon)**2) ** 0.5
        if d < best_d:
            best, best_d = name, d
    return best, round(best_d, 2)

def parse_row(row):
    utc      = row[12:35].decode("ascii", errors="replace").strip()
    orbit    = struct.unpack_from(">I", row, 39)[0]
    lat      = struct.unpack_from(">d", row, 43)[0]
    lon      = struct.unpack_from(">d", row, 51)[0]
    alt      = struct.unpack_from(">d", row, 227)[0]
    lochour  = row[235]
    locmin   = row[236]
    pointing = row[237]
    intersct = row[238]
    setn     = struct.unpack_from(">16H", row, 274)
    csetn1   = struct.unpack_from(">16H", row, 370)
    return (utc, orbit, lat, lon, alt, lochour, locmin, pointing, intersct,
            sum(setn), sum(csetn1))

# --- Load first candidate ---
with CSV_PATH.open() as f:
    reader = csv.DictReader(f)
    cand = next(reader)

path_name = cand["path_name"]
file_name = cand["file_name"].replace(".LBL", ".DAT")
url = f"{LEND_BASE}/{path_name}{file_name}"

print(f"Target : {url}")
print(f"Downloading...")
t0 = time.time()

with tempfile.NamedTemporaryFile(suffix=".dat", delete=False) as tmp:
    tmp_path = pathlib.Path(tmp.name)
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()
    total = 0
    for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MB chunks
        tmp.write(chunk)
        total += len(chunk)

elapsed = time.time() - t0
print(f"Downloaded {total/1e6:.1f} MB in {elapsed:.1f}s  →  {tmp_path}")

# --- Parse ---
print("\nParsing rows...")
data = tmp_path.read_bytes()
n_rows = len(data) // ROW_BYTES
print(f"Total rows in file : {n_rows}")

passing = []
for i in range(n_rows):
    row = data[i * ROW_BYTES : (i + 1) * ROW_BYTES]
    if len(row) < ROW_BYTES:
        break
    utc, orbit, lat, lon, alt, lochour, locmin, pointing, intersct, setn_total, csetn1_total = parse_row(row)
    if lat <= LAT_GATE and pointing == 1 and intersct == 1:
        passing.append((utc, orbit, lat, lon, alt, lochour, locmin, setn_total, csetn1_total))

print(f"Rows passing lat <= {LAT_GATE}° + quality flags : {len(passing)}")

# --- Summary ---
if passing:
    lats  = [r[2] for r in passing]
    lons  = [r[3] for r in passing]
    setns = [r[7] for r in passing]
    print(f"\nLat  range : {min(lats):.4f}° to {max(lats):.4f}°")
    print(f"Lon  range : {min(lons):.2f}° to {max(lons):.2f}°")
    print(f"SETN total range : {min(setns)} to {max(setns)} counts")
    print(f"SETN mean        : {sum(setns)/len(setns):.1f} counts")

    # --- 5 sample rows ---
    print(f"\n{'UTC':<26} {'Orbit':>6} {'Lat':>9} {'Lon':>9} {'Alt(km)':>8} "
          f"{'LocHr':>6} {'SETN':>6} {'CSETN1':>7} {'Nearest PSR'}")
    print("-" * 105)
    step = max(1, len(passing) // 5)
    shown = 0
    for idx in range(0, len(passing), step):
        if shown >= 5:
            break
        utc, orbit, lat, lon, alt, lochour, locmin, setn_t, csetn1_t = passing[idx]
        psr, dist = nearest_psr(lat, lon)
        print(f"{utc:<26} {orbit:>6} {lat:>9.4f} {lon:>9.3f} {alt:>8.2f} "
              f"{lochour:>6} {setn_t:>6} {csetn1_t:>7}   {psr} (Δ{dist}°)")
        shown += 1

    # --- PSR proximity histogram ---
    print("\nPSR proximity breakdown (nearest PSR per passing row):")
    from collections import Counter
    psr_counts = Counter(nearest_psr(r[2], r[3])[0] for r in passing)
    for psr, cnt in psr_counts.most_common():
        print(f"  {psr:<15} {cnt:>5} rows  ({100*cnt/len(passing):.1f}%)")

tmp_path.unlink()
print("\nTracer Bullet: DONE")
