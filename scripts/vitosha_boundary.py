"""
Step 1: Fetch the administrative boundary of kv. Vitosha (Sofia) from OpenStreetMap
and save it as GeoJSON.

Usage:
    python vitosha_boundary.py [output_path]

Dependencies:
    pip install osmnx>=1.7 geopandas shapely

The script uses only Latin column names and creates a file suitable for further
Geo-processing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import osmnx as ox


def fetch_vitosha_boundary(out_path: str | Path = "vitosha_boundary.geojson") -> Path:
    """Download the polygon of kv. Vitosha and save it to *GeoJSON*.

    Parameters
    ----------
    out_path: str | Path, default "vitosha_boundary.geojson"
        Where to save the boundary file.

    Returns
    -------
    Path
        Absolute path to the saved file.
    """
    print("[i] Querying OpenStreetMap for \"kv. Vitosha, Sofia, Bulgaria\" …")
    gdf: gpd.GeoDataFrame = ox.geocode_to_gdf("кв. Витоша")

    # Keep only the geometry column and set a clean name.
    gdf = gdf[["geometry"]].copy()
    gdf["name"] = "Vitosha"

    # GeoJSON prefers EPSG:4326; osmnx already returns it that way.
    out_path = Path(out_path).resolve()
    gdf.to_file(out_path, driver="GeoJSON")
    print(f"[✓] Boundary saved to {out_path}")
    return out_path


if __name__ == "__main__":
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else "vitosha_boundary.geojson"
    fetch_vitosha_boundary(output)
