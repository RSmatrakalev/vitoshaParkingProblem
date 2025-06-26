import geopandas as gpd
from pathlib import Path
import math
from typing import Union


def generate_parking_capacity() -> None:
    BASE_DIR = Path(__file__).resolve().parent.parent
    input_fp: Union[str, Path] = BASE_DIR / "output/vitosha_landparcels.geojson"
    output_fp: Union[str, Path] = BASE_DIR / "output/obshtinski_imoti_parking.geojson"

    print("[i] Зареждане на landparcels файла…")
    gdf = gpd.read_file(input_fp)

    print("[i] Филтриране на имоти по собственост и предназначение…")
    filtered = gdf[
        (gdf["proptype"].isin(["Общинска публична", "Общинска частна"]))
        & (gdf["usetype"] == "За друг вид застрояване")
        & (gdf["area"] >= 300)
    ].copy()

    print("[i] Изчисляване на потенциалните паркоместа…")
    filtered["1_flor_parking_places"] = (filtered["area"] // 30).astype(int)
    filtered["2_flor_parking_places"] = filtered["1_flor_parking_places"] * 2
    filtered["3_flor_parking_places"] = filtered["1_flor_parking_places"] * 3
    filtered["4_flor_parking_places"] = filtered["1_flor_parking_places"] * 4

    print(f"[✓] Запазване на {len(filtered)} парцела в {output_fp}…")
    output_fp.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_file(output_fp, driver="GeoJSON")
    print("[✔] Готово.")


if __name__ == "__main__":
    generate_parking_capacity()
