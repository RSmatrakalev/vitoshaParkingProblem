"""
cadaster_to_geojson.py — convert & merge КАИС кадастър
=====================================================
*Превръща всеки shapefile от КАИС в UTF‑8 GeoJSON и обединява три слоя*

Какво прави скриптът накратко
----------------------------
1. Рекурсивно обхожда **ETL/downloadsRowData/** за `*.shp`.
2. Чете всеки файл с автоматично откриване на кодировка
   (UTF‑8 → CP1251 → CP1250 → Latin‑1) и отхвърля „мо­жи­баке“.
3. Ако липсва CRS – приема **EPSG:7801** (БГС2005, зона 7); после
   прехвърля всичко в **EPSG:4326**.
4. Записва копие на всеки файл в **ETL/geojson/** за дебъг.
5. Събира обекти по слой и записва трите сурови слоя:
   `landparcels_raw.geojson`, `units_raw.geojson`, `buildings_raw.geojson`.
6. В края оправя остатъчно счупена кирилица в `units_raw.geojson`.

Стартиране
----------
```bash
python cadaster_to_geojson.py           # използва подразбиращите се ETL директории
python cadaster_to_geojson.py downloads etl_out
```
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# ------------------------------------------------------------------
# Константи
# ------------------------------------------------------------------

ENCODINGS = [None, "cp1251", "cp1250", "latin1"]  # None == „utf‑8“ по подразбиране
TARGET_CRS = "EPSG:4326"  # WGS‑84 (за GeoJSON)
FALLBACK_CRS = "EPSG:7801"  # БГС2005 / 3° GK зона 7

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DL_DIR = PROJECT_ROOT / "ETL" / "downloadsRowData"
DEFAULT_ETL_DIR = PROJECT_ROOT / "ETL"

# Българско име на подпапка → английски суфикс на изходния файл
LAYER_MAP: Dict[str, str] = {
    "pozemleni_imoti": "landparcels",
    "samostoyatelni_obekti": "units",
    "sgradi": "buildings",
}

# ------------------------------------------------------------------
# Helper-и
# ------------------------------------------------------------------

def _find_shp_files(root: Path) -> List[Path]:
    """Връща сортиран списък с всички *.shp под даден root."""
    return sorted(root.rglob("*.shp"))


def _guess_bgs2005(gdf: gpd.GeoDataFrame) -> Optional[str]:
    """Ако координатите изглеждат като БГС2005 метри → EPSG:7801."""
    if gdf.empty:
        return None
    pt: Point = gdf.geometry.iloc[0].centroid  # type: ignore[arg-type]
    if 250_000 < pt.x < 450_000 and 4_700_000 < pt.y < 4_900_000:
        return FALLBACK_CRS
    return None


def _looks_mojibake(text: str) -> bool:
    """>3 от типичните Ð, Ñ, � в първите 40 символа => счупена кирилица"""
    bad = set("ÐÑðñŽžŒœ�¿½")
    return text and sum(c in bad for c in text[:40]) > 3


def _read_with_fallback(path: Path) -> gpd.GeoDataFrame:
    """Пробва няколко кодировки, докато прочете файл без mo­ji­ba­ke."""
    last_err: Exception | None = None
    for enc in ENCODINGS:
        try:
            gdf = gpd.read_file(path, encoding=enc) if enc else gpd.read_file(path)
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue

        # тестваме една низова клетка
        sample = None
        for col in gdf.select_dtypes("object"):
            val = gdf[col].dropna().astype(str).head(1)
            if not val.empty:
                sample = val.iloc[0]
                break
        if sample and _looks_mojibake(sample):
            print(f"    ↳ {enc or 'utf-8'} изглежда счупено… опитваме следваща кодировка")
            continue
        print(f"    ↳ декодирано с {enc or 'utf-8'}; CRS = {gdf.crs}")
        break
    else:
        raise last_err  # type: ignore[arg-type]

    return gdf


def _layer_key_from_path(path: Path) -> str:
    """Вади ключа на слоя според името на папката или на файла."""
    for parent in path.parents:
        for key in LAYER_MAP:
            if key in parent.name:
                return key
    for key in LAYER_MAP:
        if key in path.stem:
            return key
    raise ValueError(f"Не мога да определя слоя за {path}")


# ------------------------------------------------------------------
# Фикс за остатъчна двойна декодировка (самостоятелни обекти)
# ------------------------------------------------------------------

def _hard_fix_cyrillic(path: Path) -> None:
    if not path.exists():
        return
    gdf = gpd.read_file(path)
    bad = set("ÐÑðñŽžŒœ�¿½")

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
    print(f"[✓] Re‑encoded Cyrillic in {path.name}")

# ------------------------------------------------------------------
# Основна логика
# ------------------------------------------------------------------

def convert_and_merge(dl_dir: Path, etl_dir: Path) -> None:
    dl_dir = dl_dir.resolve()
    etl_dir = etl_dir.resolve()
    geojson_dir = etl_dir / "geojson"
    geojson_dir.mkdir(parents=True, exist_ok=True)

    shp_files = _find_shp_files(dl_dir)
    if not shp_files:
        print(f"[!] Няма .shp файлове в {dl_dir}")
        return

    buckets: Dict[str, List[gpd.GeoDataFrame]] = {k: [] for k in LAYER_MAP}

    for shp in shp_files:
        rel = shp.relative_to(dl_dir)
        print(f"[i] Чета {rel}")
        gdf = _read_with_fallback(shp)

        # CRS обработка
        if gdf.crs is None:
            if (guess := _guess_bgs2005(gdf)) is None:
                raise ValueError(f"Неизвестен CRS за {shp}")
            gdf.set_crs(guess, inplace=True)
            print(f"    ↳ липсваше CRS — зададен {guess}")
        if gdf.crs.to_string() != TARGET_CRS:
            gdf = gdf.to_crs(TARGET_CRS)
            print(f"    ↳ репроекция → {TARGET_CRS}")

        # Записваме индивидуално GeoJSON копие
        out_individual = geojson_dir / ("_".join(rel.parts)[:-4] + ".geojson")
        gdf.to_file(out_individual, driver="GeoJSON")
        print(f"    ↳ записано {out_individual.relative_to(PROJECT_ROOT)}  ({len(gdf):,} обекти)\n")

        buckets[_layer_key_from_path(shp)].append(gdf)

    # Обединяване на слоевете
    for bg_key, frames in buckets.items():
        if not frames:
            print(f"[!] Внимание: няма кадри за {bg_key}")
            continue
        merged = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=TARGET_CRS)
        out_path = etl_dir / f"{LAYER_MAP[bg_key]}_raw.geojson"
        merged.to_file(out_path, driver="GeoJSON")
        print(f"[✓] Слоят {bg_key:<25}→ {out_path.relative_to(PROJECT_ROOT)}  ({len(merged):,} обекти)")

    # Специален фикс за units_raw.geojson
    units_path = etl_dir / "units_raw.geojson"
    _hard_fix_cyrillic(units_path)

    print("\nВсичко готово — суровите слоеве са в", etl_dir)

# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

if __name__ == "__main__":
    dl_arg = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DL_DIR
    etl_arg = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_ETL_DIR
    convert_and_merge(dl_arg, etl_arg)
