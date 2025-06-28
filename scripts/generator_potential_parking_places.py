import geopandas as gpd
from pathlib import Path
from typing import Union

def generate_parking_capacity() -> None:
    BASE_DIR = Path(__file__).resolve().parent.parent
    input_fp: Union[str, Path] = BASE_DIR / "output/vitosha_landparcels.geojson"
    output_fp: Union[str, Path] = BASE_DIR / "output/municipal_land_parking.geojson"

    print("[i] Loading land parcels file…")
    gdf = gpd.read_file(input_fp)

    print("[i] Filtering parcels by ownership and usage…")
    filtered = gdf[
        (gdf["proptype"].isin(["Общинска публична", "Общинска частна"]))
        & (gdf["usetype"] == "За друг вид застрояване")
        & (gdf["area"] >= 300)
    ].copy()

    print("[i] Estimating potential parking capacity…")
    filtered["1_floor_parking_spaces"] = (filtered["area"] // 30).astype(int)
    filtered["2_floor_parking_spaces"] = filtered["1_floor_parking_spaces"] * 2
    filtered["3_floor_parking_spaces"] = filtered["1_floor_parking_spaces"] * 3
    filtered["4_floor_parking_spaces"] = filtered["1_floor_parking_spaces"] * 4

    print(f"[✓] Saving {len(filtered)} parcels to {output_fp}…")
    output_fp.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_file(output_fp, driver="GeoJSON")
    print("[✔] Done.")

if __name__ == "__main__":
    generate_parking_capacity()
