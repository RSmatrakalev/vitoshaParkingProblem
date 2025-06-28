"""
Step 1: Fetch the administrative boundary of Vitosha district (Sofia) from OpenStreetMap
and save it as a GeoJSON file.

Usage:
    python vitosha_boundary.py [output_path]

Dependencies:
    pip install osmnx>=1.7 geopandas shapely

This script uses only Latin column names and generates a file ready
for further geospatial processing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import osmnx as ox

def fetch_vitosha_boundary(out_path: str | Path = "vitosha_boundary.geojson") -> Path:
    """Download the boundary polygon for Vitosha district and save it as GeoJSON.

    Parameters
    ----------
    out_path: str | Path, default "vitosha_boundary.geojson"
        Output file path.

    Returns
    -------
    Path
        Absolute path to the saved file.
    """
    print("[i] Querying OpenStreetMap for \"kv. Vitosha, Sofia, Bulgaria\" …")
    gdf: gpd.GeoDataFrame = ox.geocode_to_gdf("kv. Vitosha, Sofia, Bulgaria")

    gdf = gdf[["geometry"]].copy()
    gdf["name"] = "Vitosha"

    out_path = Path(out_path).resolve()
    gdf.to_file(out_path, driver="GeoJSON")
    print(f"[✓] Boundary saved to {out_path}")
    return out_path

if __name__ == "__main__":
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else "vitosha_boundary.geojson"
    fetch_vitosha_boundary(output)
