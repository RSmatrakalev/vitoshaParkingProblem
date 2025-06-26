"""
clip_and_clean_vitosha.py — Steps 3–5 (rev2)
============================================
1. Load the *Vitosha* boundary (searches recursively for **vitosha_boundary.geojson**
   inside the given *etl_dir* so you can keep it in *ETL/parsedJsons/* or similar).
2. Load the merged *raw* cadastral layers (landparcels, units, buildings).
3. Spatial **clip** each layer to the neighbourhood polygon.
4. **Select & rename** only the columns we need (lower‑case, ASCII).
5. Save three cleaned GeoJSON files ready for web/app use.

Output
------
```
output/
    vitosha_landparcels.geojson
    vitosha_units.geojson
    vitosha_buildings.geojson
```

Usage
-----
```
python clip_and_clean_vitosha.py [etl_dir] [output_dir]
```
Defaults:
* etl_dir     → ETL/
* output_dir  → output/

Dependencies
------------
```
pip install geopandas shapely
```
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import geopandas as gpd

# ---------------------------------------------------------------------
# Paths & Constants
# ---------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ETL_DIR = PROJECT_ROOT / "ETL"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output"
BOUNDARY_NAME = "vitosha_boundary.geojson"

# layer_key -> (raw filename, cleaned filename, keep & rename mapping)
LAYER_CFG: Dict[str, Dict] = {
    "landparcels": {
        "in": "landparcels_raw.geojson",
        "out": "vitosha_landparcels.geojson",
        "cols": {
            "cadnum": "cadnum",
            "AREA": "area",
            "proptype": "proptype",
            "purpcode": "purpcode",
            "purptype": "purptype",
            "quarname": "quarname",
            "strename": "strename",
            "strnum": "strnum",
            "usetype": "usetype",
            "ekattefn": "city",
        },
    },
    "units": {
        "in": "units_raw.geojson",
        "out": "vitosha_units.geojson",
        "cols": {
            "cadnum": "cadnum",
            "AREA": "area",
            "apparea": "apparea",
            "apptype": "apptype",
            "strename": "strename",
            "strnum": "strnum",
        },
    },
    "buildings": {
        "in": "buildings_raw.geojson",
        "out": "vitosha_buildings.geojson",
        "cols": {
            "cadnum": "cadnum",
            "appcount": "appcount",
            "flrcount": "flrcount",
            "functype": "functype",
            "strename": "strename",
            "strnum": "strnum",
        },
    },
}

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def _find_boundary(etl_dir: Path) -> Path:
    """Return the first path matching *BOUNDARY_NAME* inside *etl_dir*."""
    matches: List[Path] = list(etl_dir.rglob(BOUNDARY_NAME))
    if not matches:
        raise SystemExit(f"Boundary '{BOUNDARY_NAME}' not found under {etl_dir}")
    return matches[0]


def _load_boundary(etl_dir: Path) -> gpd.GeoDataFrame:
    bnd_path = _find_boundary(etl_dir)
    print(f"[i] Boundary found: {bnd_path.relative_to(etl_dir)}")
    return gpd.read_file(bnd_path).to_crs("EPSG:4326")


def _clip_clean_save(layer_key: str, cfg: Dict, boundary: gpd.GeoDataFrame, etl_dir: Path, out_dir: Path) -> None:
    """Clip raw layer to boundary, keep/rename columns, save to GeoJSON."""
    raw_path = etl_dir / cfg["in"]
    if not raw_path.exists():
        # Fallback: maybe raw files are inside parsedJsons or another subfolder.
        hits = list(etl_dir.rglob(cfg["in"]))
        if hits:
            raw_path = hits[0]
        else:
            raise SystemExit(f"Missing raw layer '{cfg['in']}' under {etl_dir}")

    gdf = gpd.read_file(raw_path)
    if gdf.crs != boundary.crs:
        gdf = gdf.to_crs(boundary.crs)

    clipped = gpd.clip(gdf, boundary)

    # Select & rename columns; geometry retained implicitly.
    cols_map = cfg["cols"]
    missing = set(cols_map.keys()) - set(clipped.columns)
    if missing:
        raise ValueError(f"{layer_key}: columns missing in source: {missing}")

    cleaned = clipped[list(cols_map.keys()) + ["geometry"]].rename(columns=cols_map)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / cfg["out"]
    cleaned.to_file(out_path, driver="GeoJSON")
    print(f"[✓] {layer_key:<12} → {out_path.relative_to(PROJECT_ROOT)}  ({len(cleaned):,} features)")


# ---------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------

def main(etl_dir: Path = DEFAULT_ETL_DIR, out_dir: Path = DEFAULT_OUT_DIR) -> None:
    etl_dir = Path(etl_dir).resolve()
    out_dir = Path(out_dir).resolve()

    boundary = _load_boundary(etl_dir)
    print(f"[i] Boundary loaded; CRS = {boundary.crs}")

    for key, cfg in LAYER_CFG.items():
        _clip_clean_save(key, cfg, boundary, etl_dir, out_dir)

    print("\nAll done — cleaned layers are in", out_dir)


if __name__ == "__main__":
    etl_arg = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ETL_DIR
    out_arg = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUT_DIR
    main(etl_arg, out_arg)
