#!/usr/bin/env python3
"""
vitosha_analytics.py — Parking capacity vs. demand analysis for Vitosha district
===============================================================================
Calculates, per cadastral parcel (based on the first three segments of
*cadnum*), how many parking spaces are available and how many are needed
according to local regulations.

Inputs (expected in the same directory):
  • vitosha_buildings.geojson  – building footprints
  • vitosha_units.geojson      – units (apartments, garages, etc.)

Output:
  • vitosha_parking_num.geojson – buildings enriched with:
      cadnum            – cadastral ID
      num_apartments    – residential units in the parcel
      num_garages       – garage units in the parcel
      sum_parking_place – available parking spaces
      sum_needed_place  – required parking spaces based on area

Usage:
------
    python vitosha_analytics.py
    python vitosha_analytics.py buildings.gjson units.gjson output.gjson
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import pandas as pd

def base_cadnum(cadnum: str) -> str:
    """Extract first three segments of a cadastral number."""
    if not cadnum or not isinstance(cadnum, str):
        return ""
    return ".".join(cadnum.split(".")[:3])

def needed_places(area: float | int | None) -> int:
    """Determine required parking: 1 if <90 m², else 2."""
    try:
        return 1 if float(area) < 90.0 else 2
    except (TypeError, ValueError):
        return 1

def compute_parcel_stats(units: gpd.GeoDataFrame) -> pd.DataFrame:
    """Compute parking statistics per cadastral parcel."""
    units = units.copy()
    units["parcel_id"] = units["cadnum"].apply(base_cadnum)

    is_apartment = units["apptype"].str.contains("Жилище", na=False, case=False)
    is_garage = units["apptype"].str.contains("Гараж", na=False, case=False)

    apartments = units[is_apartment]
    garages = units[is_garage]

    apartment_counts = apartments.groupby("parcel_id").size().rename("num_apartments")
    garage_counts = garages.groupby("parcel_id").size().rename("num_garages")

    apartments["req"] = apartments["area"].apply(needed_places)
    demand = apartments.groupby("parcel_id")["req"].sum().rename("sum_needed_place")

    stats = (
        pd.concat([apartment_counts, garage_counts, demand], axis=1)
        .fillna(0)
        .astype(int)
        .reset_index()
        .rename(columns={"parcel_id": "cadnum"})
    )
    stats["sum_parking_place"] = stats[["num_apartments", "num_garages"]].max(axis=1)

    return stats

def main(
    buildings_fp: str | Path = Path(__file__).resolve().parent.parent / "output/vitosha_buildings.geojson",
    units_fp: str | Path = Path(__file__).resolve().parent.parent / "output/vitosha_units.geojson",
    output_fp: str | Path = Path(__file__).resolve().parent.parent / "output/vitosha_parking_num.geojson",
) -> None:
    buildings_fp = Path(buildings_fp)
    units_fp = Path(units_fp)
    output_fp = Path(output_fp)

    print("[i] Loading input data…")
    buildings = gpd.read_file(buildings_fp)
    units = gpd.read_file(units_fp)

    print("[i] Calculating parcel statistics…")
    parcel_stats = compute_parcel_stats(units)

    buildings = buildings.copy()
    buildings["parcel_id"] = buildings["cadnum"].apply(base_cadnum)

    print("[i] Merging statistics with buildings layer…")
    enriched = buildings.merge(
        parcel_stats.rename(columns={"cadnum": "parcel_id"}),
        on="parcel_id",
        how="left",
    )

    for col in ["num_apartments", "num_garages", "sum_parking_place", "sum_needed_place"]:
        if col in enriched.columns:
            enriched[col] = enriched[col].fillna(0).astype(int)

    enriched.drop(columns=["parcel_id"], inplace=True)

    print(f"[✓] Writing output with {len(enriched):,} features → {output_fp}")
    enriched.to_file(output_fp, driver="GeoJSON")
    print("[✔] Done.")

if __name__ == "__main__":
    argv = sys.argv[1:]
    main(*argv)
