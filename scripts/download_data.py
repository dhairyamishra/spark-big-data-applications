#!/usr/bin/env python3
"""
Download NYC TLC trip data (Parquet) and taxi zone reference files.
Usage:
    python download_data.py                    # Full 2023-2024 dataset
    python download_data.py --years 2024       # Single year
    python download_data.py --types yellow     # Single taxi type
    python download_data.py --months 1 2 3     # Specific months
"""

import argparse
import os
import sys
import requests
from tqdm import tqdm

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
from config import (
    TLC_CDN, ZONE_LOOKUP_URL, ZONE_SHAPEFILE_URL,
    YEARS, MONTHS, TAXI_TYPES, LOCAL_DATA_DIR, LOCAL_ZONE_DIR,
)


def download_file(url: str, dest: str, chunk_size: int = 8192) -> bool:
    """Download a file with progress bar. Returns True on success."""
    if os.path.exists(dest):
        print(f"  [SKIP] {dest} already exists")
        return True

    os.makedirs(os.path.dirname(dest), exist_ok=True)
    try:
        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [FAIL] {url} → {e}")
        return False

    total = int(resp.headers.get("content-length", 0))
    fname = os.path.basename(dest)

    with open(dest, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc=f"  {fname}", leave=True
    ) as bar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            f.write(chunk)
            bar.update(len(chunk))

    return True


def download_trip_data(years: list, months: list, taxi_types: list):
    """Download TLC Parquet files for specified years/months/types."""
    for ttype in taxi_types:
        base_url = TAXI_TYPES[ttype]
        for year in years:
            for month in months:
                filename = f"{ttype}_tripdata_{year}-{month:02d}.parquet"
                url = f"{base_url}_{year}-{month:02d}.parquet"
                dest = os.path.join(LOCAL_DATA_DIR, ttype, str(year), filename)
                print(f"\n[{ttype.upper()}] {year}-{month:02d}")
                download_file(url, dest)


def download_zone_files():
    """Download taxi zone lookup CSV and shapefile."""
    os.makedirs(LOCAL_ZONE_DIR, exist_ok=True)

    print("\n[ZONES] Downloading taxi zone lookup CSV...")
    download_file(ZONE_LOOKUP_URL, os.path.join(LOCAL_ZONE_DIR, "taxi_zone_lookup.csv"))

    print("\n[ZONES] Downloading taxi zone shapefile...")
    download_file(ZONE_SHAPEFILE_URL, os.path.join(LOCAL_ZONE_DIR, "taxi_zones.zip"))


def main():
    parser = argparse.ArgumentParser(description="Download NYC TLC trip data")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS,
                        help="Years to download (default: 2023 2024)")
    parser.add_argument("--months", nargs="+", type=int, default=MONTHS,
                        help="Months to download (default: 1-12)")
    parser.add_argument("--types", nargs="+", default=list(TAXI_TYPES.keys()),
                        choices=list(TAXI_TYPES.keys()),
                        help="Taxi types to download (default: all)")
    parser.add_argument("--skip-zones", action="store_true",
                        help="Skip downloading zone reference files")
    args = parser.parse_args()

    print("=" * 60)
    print("  NYC TLC Trip Data Downloader")
    print(f"  Years:  {args.years}")
    print(f"  Months: {args.months}")
    print(f"  Types:  {args.types}")
    print("=" * 60)

    if not args.skip_zones:
        download_zone_files()

    download_trip_data(args.years, args.months, args.types)

    print("\n" + "=" * 60)
    print("  Download complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
