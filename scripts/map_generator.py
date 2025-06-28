import geopandas as gpd
import folium
from shapely.geometry import box
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent / "output"
PARKING_NUM_FP = BASE / "vitosha_parking_num.geojson"
PARKED_CARS_FP = BASE / "parked_cars_vitosha.geojson"
MUNICIPAL_FP = BASE / "municipal_land_parking.geojson"
OUTPUT_HTML = BASE / "vitosha_parking_heatmap.html"

BUFFER_RADIUS = 50
GRID_CELLS = 800

print("[i] Loading input GeoJSON files…")
buildings = gpd.read_file(PARKING_NUM_FP).to_crs(epsg=3857)
cars = gpd.read_file(PARKED_CARS_FP).to_crs(epsg=3857)
municipal = gpd.read_file(MUNICIPAL_FP).to_crs(epsg=3857)

buildings["buffer_geom"] = buildings.geometry.buffer(BUFFER_RADIUS)
buildings["cars_in_buffer"] = buildings["buffer_geom"].apply(lambda buf: cars.within(buf).sum())
buildings["total_supply"] = buildings["num_garages"] + buildings["cars_in_buffer"]
buildings["parking_deficit"] = (buildings["sum_needed_place"] - buildings["total_supply"]).clip(lower=0)

print("[i] Creating grid…")
total_bounds = buildings.total_bounds
minx, miny, maxx, maxy = total_bounds
width = maxx - minx
height = maxy - miny
cell_area = (width * height) / GRID_CELLS
cell_size = (cell_area)**0.5
cols = int(width // cell_size)
rows = int(GRID_CELLS // cols) + 1

grid_cells = [
    box(minx + i * cell_size, miny + j * cell_size,
        minx + (i + 1) * cell_size, miny + (j + 1) * cell_size)
    for i in range(cols) for j in range(rows)
]

grid = gpd.GeoDataFrame(geometry=grid_cells, crs=buildings.crs)

joined = gpd.sjoin(buildings.set_geometry("geometry"), grid, how="inner", predicate="intersects")
cell_deficit = joined.groupby("index_right")["parking_deficit"].sum().reset_index()

grid["parking_deficit"] = 0
grid.loc[cell_deficit["index_right"], "parking_deficit"] = cell_deficit["parking_deficit"].values

print("[i] Generating heatmap…")
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
            tooltip=f"Deficit: {row['parking_deficit']}"
        ).add_to(m)

for _, row in municipal.to_crs(epsg=4326).iterrows():
    tooltip_text = (
        f"Municipal Property\n"
        f"1 floor: {row.get('1_flor_parking_places', 'n/a')}\n"
        f"2 floors: {row.get('2_flor_parking_places', 'n/a')}\n"
        f"3 floors: {row.get('3_flor_parking_places', 'n/a')}\n"
        f"4 floors: {row.get('4_flor_parking_places', 'n/a')}"
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
        f"Building\n"
        f"Type: {row.get('functype', 'n/a')}\n"
        f"Available (garage + street): {row['total_supply']}\n"
        f"Needed: {row['sum_needed_place']}\n"
        f"Deficit: {row['parking_deficit']}"
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
print(f"[✓] Map saved to: {OUTPUT_HTML}")
