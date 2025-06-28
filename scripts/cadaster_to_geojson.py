from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

ENCODINGS = [None, "cp1251", "cp1250", "latin1"]  # None defaults to "utf-8"
TARGET_CRS = "EPSG:4326"  # WGS-84 for GeoJSON
FALLBACK_CRS = "EPSG:7801"  # BGS2005 / 3° GK zone 7

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DL_DIR = PROJECT_ROOT / "ETL" / "downloadsRowData"
DEFAULT_ETL_DIR = PROJECT_ROOT / "ETL"

LAYER_MAP: Dict[str, str] = {
    "pozemleni_imoti": "landparcels",
    "samostoyatelni_obekti": "units",
    "sgradi": "buildings",
}

def _find_shp_files(root: Path) -> List[Path]:
    return sorted(root.rglob("*.shp"))

def _guess_bgs2005(gdf: gpd.GeoDataFrame) -> Optional[str]:
    if gdf.empty:
        return None
    pt: Point = gdf.geometry.iloc[0].centroid  # type: ignore[arg-type]
    if 250_000 < pt.x < 450_000 and 4_700_000 < pt.y < 4_900_000:
        return FALLBACK_CRS
    return None

def _looks_mojibake(text: str) -> bool:
    bad = set("ÐÑðñŽžŒœ¿½�")
    return text and sum(c in bad for c in text[:40]) > 3

def _read_with_fallback(path: Path) -> gpd.GeoDataFrame:
    last_err: Exception | None = None
    for enc in ENCODINGS:
        try:
            gdf = gpd.read_file(path, encoding=enc) if enc else gpd.read_file(path)
        except Exception as e:
            last_err = e
            continue

        sample = None
        for col in gdf.select_dtypes("object"):
            val = gdf[col].dropna().astype(str).head(1)
            if not val.empty:
                sample = val.iloc[0]
                break
        if sample and _looks_mojibake(sample):
            print(f"    ↳ {enc or 'utf-8'} appears corrupted, trying next")
            continue
        print(f"    ↳ decoded with {enc or 'utf-8'}; CRS = {gdf.crs}")
        break
    else:
        raise last_err  # type: ignore[arg-type]

    return gdf

def _layer_key_from_path(path: Path) -> str:
    for parent in path.parents:
        for key in LAYER_MAP:
            if key in parent.name:
                return key
    for key in LAYER_MAP:
        if key in path.stem:
            return key
    raise ValueError(f"Unable to determine layer for {path}")

def _hard_fix_cyrillic(path: Path) -> None:
    if not path.exists():
        return
    gdf = gpd.read_file(path)
    bad = set("\u00d0\u00d1\u00f0\u00f1\u017d\u017e\u0152\u0153\u00bf\u00bd\uFFFD")

    def looks_bad(s: str) -> bool:
        return s and sum(c in bad for c in s[:40]) > 3

    def attempt(val: str) -> str:
        combos = [
            ("cp1251", "utf-8"),
            ("utf-8", "cp1251"),
            ("latin1", "utf-8"),
            ("utf-8", "latin1"),
        ]
        for enc, dec in combos:
            try:
                fixed = val.encode(enc, "ignore").decode(dec, "ignore")
                if not looks_bad(fixed):
                    return fixed
            except UnicodeError:
                continue
        return val

    for col in gdf.select_dtypes(include="object"):
        sample = str(gdf[col].iloc[0]) if not gdf[col].empty else ""
        if looks_bad(sample):
            gdf[col] = gdf[col].astype(str).apply(attempt)

    gdf.to_file(path, driver="GeoJSON")
    print(f"[✓] Re-encoded Cyrillic in {path.name}")

def convert_and_merge(dl_dir: Path, etl_dir: Path) -> None:
    dl_dir = dl_dir.resolve()
    etl_dir = etl_dir.resolve()
    geojson_dir = etl_dir / "geojson"
    geojson_dir.mkdir(parents=True, exist_ok=True)

    shp_files = _find_shp_files(dl_dir)
    if not shp_files:
        print(f"[!] No .shp files found in {dl_dir}")
        return

    buckets: Dict[str, List[gpd.GeoDataFrame]] = {k: [] for k in LAYER_MAP}

    for shp in shp_files:
        rel = shp.relative_to(dl_dir)
        print(f"[i] Reading {rel}")
        gdf = _read_with_fallback(shp)

        if gdf.crs is None:
            if (guess := _guess_bgs2005(gdf)) is None:
                raise ValueError(f"Missing CRS for {shp}")
            gdf.set_crs(guess, inplace=True)
            print(f"    ↳ CRS set to {guess}")
        if gdf.crs.to_string() != TARGET_CRS:
            gdf = gdf.to_crs(TARGET_CRS)
            print(f"    ↳ reprojected to {TARGET_CRS}")

        out_individual = geojson_dir / ("_".join(rel.parts)[:-4] + ".geojson")
        gdf.to_file(out_individual, driver="GeoJSON")
        print(f"    ↳ saved {out_individual.relative_to(PROJECT_ROOT)}  ({len(gdf):,} features)\n")

        buckets[_layer_key_from_path(shp)].append(gdf)

    for bg_key, frames in buckets.items():
        if not frames:
            print(f"[!] Warning: no frames for {bg_key}")
            continue
        merged = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=TARGET_CRS)
        out_path = etl_dir / f"{LAYER_MAP[bg_key]}_raw.geojson"
        merged.to_file(out_path, driver="GeoJSON")
        print(f"[✓] Layer {bg_key:<25}→ {out_path.relative_to(PROJECT_ROOT)}  ({len(merged):,} features)")

    units_path = etl_dir / "units_raw.geojson"
    _hard_fix_cyrillic(units_path)

    print("\nDone — raw layers saved in", etl_dir)

if __name__ == "__main__":
    dl_arg = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DL_DIR
    etl_arg = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_ETL_DIR
    convert_and_merge(dl_arg, etl_arg)
