# created_by_agent
# Tracer Bullet: fetch one LEND_RDR_RSCI .LBL file and extract bounding coordinates.
# Run locally: python src/test_label_scraper.py

import csv, re, requests, pathlib

LEND_BASE = "https://pds-geosciences.wustl.edu/lro/lro-l-lend-2-edr-v1/lrolen_0xxx"
CSV_PATH  = pathlib.Path(__file__).parent.parent / "config" / "rsci_paths.csv"

LAT_RE = re.compile(r"MINIMUM_LATITUDE\s*=\s*(-?\d+\.?\d*)")
LON_RE = re.compile(r"MINIMUM_LONGITUDE\s*=\s*(-?\d+\.?\d*)")
MAX_LAT_RE = re.compile(r"MAXIMUM_LATITUDE\s*=\s*(-?\d+\.?\d*)")
MAX_LON_RE = re.compile(r"MAXIMUM_LONGITUDE\s*=\s*(-?\d+\.?\d*)")

with CSV_PATH.open() as f:
    reader = csv.DictReader(f)
    row = next(reader)

path_name = row["path_name"]
file_name = row["file_name"]
url = f"{LEND_BASE}/{path_name}{file_name}"

print(f"Fetching: {url}")
r = requests.get(url, timeout=30)
r.raise_for_status()
text = r.text

min_lat = LAT_RE.search(text)
max_lat = MAX_LAT_RE.search(text)
min_lon = LON_RE.search(text)
max_lon = MAX_LON_RE.search(text)

print(f"\n--- Parsed coordinates ---")
print(f"MINIMUM_LATITUDE  = {min_lat.group(1) if min_lat else 'NOT FOUND'}")
print(f"MAXIMUM_LATITUDE  = {max_lat.group(1) if max_lat else 'NOT FOUND'}")
print(f"MINIMUM_LONGITUDE = {min_lon.group(1) if min_lon else 'NOT FOUND'}")
print(f"MAXIMUM_LONGITUDE = {max_lon.group(1) if max_lon else 'NOT FOUND'}")
print(f"\nHTTP status: {r.status_code}  size: {len(r.content)} bytes")
print(f"\n--- Raw label excerpt (first 60 lines) ---")
for i, line in enumerate(text.splitlines()):
    if i >= 60:
        break
    print(line)
