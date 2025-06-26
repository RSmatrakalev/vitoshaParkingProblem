import geopandas as gpd
import folium
from shapely.geometry import box
from pathlib import Path

# Пътища към файловете (спрямо местоположението на скрипта)
BASE = Path(__file__).resolve().parent.parent / "output"
PARKING_NUM_FP = BASE / "vitosha_parking_num.geojson"
PARKED_CARS_FP = BASE / "parked_cars_vitosha.geojson"
OBSHTINSKI_FP = BASE / "obshtinski_imoti_parking.geojson"
OUTPUT_HTML = BASE / "vitosha_parking_heatmap.html"

BUFFER_RADIUS = 50
GRID_CELLS = 800

print("[i] Зареждам входните GeoJSON файлове…")
buildings = gpd.read_file(PARKING_NUM_FP).to_crs(epsg=3857)
cars = gpd.read_file(PARKED_CARS_FP).to_crs(epsg=3857)
public_lands = gpd.read_file(OBSHTINSKI_FP).to_crs(epsg=3857)

buildings["buffer_geom"] = buildings.geometry.buffer(BUFFER_RADIUS)
buildings["cars_in_buffer"] = buildings["buffer_geom"].apply(lambda buf: cars.within(buf).sum())
buildings["total_supply"] = buildings["num_garages"] + buildings["cars_in_buffer"]
buildings["parking_deficit"] = (buildings["sum_needed_place"] - buildings["total_supply"]).clip(lower=0)

print("[i] Създавам грид с 10 000 клетки…")
total_bounds = buildings.total_bounds
minx, miny, maxx, maxy = total_bounds
width = maxx - minx
height = maxy - miny
cell_area = (width * height) / GRID_CELLS
cell_size = (cell_area)**0.5
cols = int(width // cell_size)
rows = int(GRID_CELLS // cols) + 1

grid_cells = []
for i in range(cols):
    for j in range(rows):
        cell = box(
            minx + i * cell_size,
            miny + j * cell_size,
            minx + (i + 1) * cell_size,
            miny + (j + 1) * cell_size,
        )
        grid_cells.append(cell)

grid = gpd.GeoDataFrame(geometry=grid_cells, crs=buildings.crs)

joined = gpd.sjoin(buildings.set_geometry("geometry"), grid, how="inner", predicate="intersects")
cell_deficit = joined.groupby("index_right")["parking_deficit"].sum().reset_index()

grid["parking_deficit"] = 0
grid.loc[cell_deficit["index_right"], "parking_deficit"] = cell_deficit["parking_deficit"].values

print("[i] Генерирам heatmap карта…")
center_geom = buildings.to_crs(epsg=4326).geometry.unary_union.centroid
map_center = [center_geom.y, center_geom.x]
m = folium.Map(location=map_center, zoom_start=15, tiles="cartodbpositron")

def color_scale(val, vmin, vmax):
    adjusted_max = max(vmax, 100)
    ratio = min(max(val / adjusted_max, 0), 1)
    r = 255
    g = int(255 * (1 - ratio))
    return f"#{r:02x}{g:02x}00"

vmin, vmax = grid["parking_deficit"].min(), grid["parking_deficit"].max()

for _, row in grid.to_crs(epsg=4326).iterrows():
    if row["parking_deficit"] > 0:
        folium.GeoJson(
            row.geometry,
            style_function=lambda x, val=row["parking_deficit"]: {
                "fillColor": color_scale(val, vmin, vmax),
                "color": "black",
                "weight": 0.2,
                "fillOpacity": 0.5,
            },
            tooltip=f"Нужда: {row['parking_deficit']}"
        ).add_to(m)

for _, row in public_lands.to_crs(epsg=4326).iterrows():
    tooltip_text = (
        f"Общински имот\n"
        f"1 етаж: {row.get('1_flor_parking_places', 'н/д')} места\n"
        f"2 етажа: {row.get('2_flor_parking_places', 'н/д')}\n"
        f"3 етажа: {row.get('3_flor_parking_places', 'н/д')}\n"
        f"4 етажа: {row.get('4_flor_parking_places', 'н/д')}"
    )
    folium.GeoJson(
        row.geometry,
        style_function=lambda x: {
            "fillColor": "green",
            "color": "black",
            "weight": 0.5,
            "fillOpacity": 0.6,
        },
        tooltip=tooltip_text
    ).add_to(m)

for _, row in buildings.to_crs(epsg=4326).iterrows():
    tooltip_text = (
        f"Сграда\n"
        f"Тип: {row.get('functype', 'н/д')}\n"
        f"Налични места (гаражи+улица): {row['total_supply']}\n"
        f"Нужни места: {row['sum_needed_place']}\n"
        f"Недостиг: {row['parking_deficit']}"
    )
    folium.GeoJson(
        row.geometry,
        style_function=lambda x: {
            "fillColor": "blue",
            "color": "black",
            "weight": 0.3,
            "fillOpacity": 0.5,
        },
        tooltip=tooltip_text
    ).add_to(m)

m.save(str(OUTPUT_HTML))
print(f"[✓] Картата е запазена в: {OUTPUT_HTML}")
