#!/usr/bin/env python3
"""
vitosha_analytics.py — Parking capacity vs. demand for kv. Vitosha
================================================================
Calculates, per cadastral parcel (first three dot‑separated components of
*cadnum*), how many parking places exist and how many are needed based on
Bulgarian regulations.

Inputs (must exist in the same folder):
  • vitosha_buildings.geojson  – buildings layer
  • vitosha_units.geojson      – units (apartments, garages, etc.) layer
  • vitosha_landparcels.geojson (unused, only for reference)

Output:
  • vitosha_parking_num.geojson – buildings geometry with 4 new attributes:
      cadnum            – parcel id (68134.905.1006)
      num_apartments    – residential units in parcel
      num_garages       – garage units in parcel
      sum_parking_place – available parking places (max of apartments / garages)
      sum_needed_place  – required parking places (1 per <90 m² flat, 2 otherwise)

Usage
-----
Simply run the script in the directory that contains the GeoJSON files:

    python vitosha_analytics.py

You can also pass custom input/output paths:

    python vitosha_analytics.py buildings.gjson units.gjson output.gjson
"""

from __future__ import annotations

import sys
from pathlib import Path
import re
from typing import Tuple

import geopandas as gpd
import pandas as pd

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def base_cadnum(cadnum: str) -> str:
    """Return the first three dot‑separated elements of the cadastral number."""
    # Defensive: handle None / unexpected input
    if not cadnum or not isinstance(cadnum, str):
        return ""
    return ".".join(cadnum.split(".")[:3])


def needed_places(area: float | int | None) -> int:
    """Cars required by flat area: <90 m² → 1, ≥90 m² → 2."""
    try:
        return 1 if float(area) < 90.0 else 2
    except (TypeError, ValueError):
        # Missing or bad area – assume one place just in case
        return 1


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def compute_parcel_stats(units: gpd.GeoDataFrame) -> pd.DataFrame:
    """Return DataFrame with parking supply & demand per parcel."""
    units = units.copy()
    units["parcel_id"] = units["cadnum"].apply(base_cadnum)

    # Residential apartments
    mask_apart = units["apptype"].str.contains(r"Жилище", na=False, case=False)
    apartments = units[mask_apart]

    # Garages (could include underground, box, etc.)
    mask_garage = units["apptype"].str.contains(r"Гараж", na=False, case=False)
    garages = units[mask_garage]

    # Counts per parcel
    apart_cnt = apartments.groupby("parcel_id").size().rename("num_apartments")
    garage_cnt = garages.groupby("parcel_id").size().rename("num_garages")

    # Demand: 1 or 2 places per flat depending on area
    apartments["req"] = apartments["area"].apply(needed_places)
    demand = apartments.groupby("parcel_id")["req"].sum().rename("sum_needed_place")

    # Combine & derive available places (supply)
    stats = (
        pd.concat([apart_cnt, garage_cnt, demand], axis=1)
        .fillna(0)
        .astype(int)
        .reset_index()
        .rename(columns={"parcel_id": "cadnum"})
    )
    stats["sum_parking_place"] = stats[["num_apartments", "num_garages"]].max(axis=1)

    return stats


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

def main(
    buildings_fp: str | Path = Path(__file__).resolve().parent.parent / "output/vitosha_buildings.geojson",
    units_fp: str | Path = Path(__file__).resolve().parent.parent / "output/vitosha_units.geojson",
    output_fp: str | Path = Path(__file__).resolve().parent.parent / "output/vitosha_parking_num.geojson",
) -> None:
    buildings_fp = Path(buildings_fp)
    units_fp = Path(units_fp)
    output_fp = Path(output_fp)

    print("[i] Loading GeoJSON layers…")
    buildings = gpd.read_file(buildings_fp)
    units = gpd.read_file(units_fp)

    print("[i] Computing parking statistics per parcel…")
    parcel_stats = compute_parcel_stats(units)

    # Prepare buildings – attach parcel id column for the join
    buildings = buildings.copy()
    buildings["parcel_id"] = buildings["cadnum"].apply(base_cadnum)

    print("[i] Joining statistics to buildings layer…")
    enriched = buildings.merge(
        parcel_stats.rename(columns={"cadnum": "parcel_id"}),
        on="parcel_id",
        how="left",
    )

    # Replace NaNs with zeros for integer cols
    for col in [
        "num_apartments",
        "num_garages",
        "sum_parking_place",
        "sum_needed_place",
    ]:
        if col in enriched.columns:
            enriched[col] = enriched[col].fillna(0).astype(int)

    # Keep original cadnum but drop helper column
    enriched.drop(columns=["parcel_id"], inplace=True)

    print(f"[✓] Saving {len(enriched):,} features → {output_fp}")
    enriched.to_file(output_fp, driver="GeoJSON")
    print("[✔] Done.")


if __name__ == "__main__":
    # Allow quick CLI overrides: python vitosha_analytics.py b.gjson u.gjson out.gjson
    argv = sys.argv[1:]
    main(*argv)
