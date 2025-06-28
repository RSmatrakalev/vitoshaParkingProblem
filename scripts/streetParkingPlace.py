import osmnx as ox
import geopandas as gpd
from shapely.geometry import LineString

PLACE = "Vitosha district, Sofia, Bulgaria"
CAR_SLOT = 5.0
CAR_LENGTH = 4.5
BUF_CROSS_STOP = 25
BUF_DRIVEWAY = 10
MAX_SEGMENT_LENGTH = 100
MAX_CAPACITY = 14
BUFFER_GAP = 0.5

OUT_LINES = "street_capacity_vitosha.geojson"
OUT_CARS = "parked_cars_vitosha.geojson"

# Load district boundary
district = ox.geocode_to_gdf(PLACE).iloc[0].geometry

# Load driveable street network (small streets only)
G = ox.graph_from_polygon(district, network_type="drive")
edges = ox.graph_to_gdfs(G, nodes=False)

ban_highways = ["primary", "secondary", "tertiary", "trunk", "motorway", "unclassified"]
edges = edges[~edges["highway"].isin(ban_highways)]

# Load objects that block parking
tags_block = {"highway": ["crossing", "bus_stop"]}
ban_nodes = ox.features_from_polygon(district, tags_block)

# Load driveways
driveways = ox.features_from_polygon(district, {"service": "driveway"})
driveways = driveways[driveways.geom_type.isin(["LineString", "MultiLineString"])]

# Project to meters
edges = edges.to_crs(3857)
ban_nodes = ban_nodes.to_crs(3857)
driveways = driveways.to_crs(3857)

ban_zone = ban_nodes.geometry.buffer(BUF_CROSS_STOP).union_all()
drive_zone = driveways.geometry.buffer(BUF_DRIVEWAY).union_all()

# Remove areas near crossings and driveways
edges["geometry"] = edges.geometry.difference(ban_zone).difference(drive_zone)
edges = edges[~edges.geometry.is_empty & edges.geometry.notna()]

# Filter segments by length and capacity
edges["length_m"] = edges.geometry.length
edges["capacity"] = (edges["length_m"] // CAR_SLOT).astype(int)
edges = edges[
    (edges["length_m"] <= MAX_SEGMENT_LENGTH) & 
    (edges["capacity"] <= MAX_CAPACITY) &
    (edges["capacity"] >= 1)
]

# Generate parking segments per car
cars = []
for idx, row in edges.iterrows():
    geom = row.geometry
    parts = geom.geoms if geom.geom_type == "MultiLineString" else [geom]

    for part in parts:
        length = part.length
        n_cars = int(length // CAR_SLOT)

        for i in range(n_cars):
            start_dist = i * CAR_SLOT + (BUFFER_GAP / 2)
            end_dist = start_dist + CAR_LENGTH - BUFFER_GAP

            if end_dist <= length:
                car_line = LineString([
                    part.interpolate(start_dist),
                    part.interpolate(end_dist)
                ])
                cars.append({"segment": idx, "geometry": car_line})

cars = gpd.GeoDataFrame(cars, crs=edges.crs)

# Save outputs
edges.to_crs(4326).to_file(OUT_LINES, driver="GeoJSON")
cars.to_crs(4326).to_file(OUT_CARS, driver="GeoJSON")

print(f"[✓] Finished: {OUT_LINES} / {OUT_CARS}")
print(f"Total segments: {len(edges)}")
print(f"Total parking spaces: {len(cars)}")
