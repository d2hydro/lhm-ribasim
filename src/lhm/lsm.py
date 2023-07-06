import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

def snap_to_waterbodies(lsm_lhm_gdf: gpd.GeoDataFrame, waterbodies_gdf: gpd.GeoDataFrame, offset=100):

    sindex = waterbodies_gdf.sindex
    lsm_lhm_snapped_gdf = lsm_lhm_gdf.copy(deep=True)
    lsm_lhm_snapped_gdf["geometry_edit"] = None
    
    for row in lsm_lhm_snapped_gdf.itertuples():
        indices = sindex.intersection(row.geometry.bounds)
        distance_series = waterbodies_gdf.loc[indices].distance(row.geometry)
        if distance_series.min() > 0:
            if distance_series.min() <= offset:
                idx = distance_series.sort_values().index[0]
                poly = waterbodies_gdf.at[idx, "geometry"]
                point = row.geometry
                lsm_lhm_snapped_gdf.at[row.Index, "geometry"] = poly.boundary.interpolate(
                    poly.boundary.project(point)
                    )
                lsm_lhm_snapped_gdf.at[row.Index, "geometry_edit"] = "snapped"
            else:
                lsm_lhm_snapped_gdf.at[row.Index, "geometry_edit"] = f">{offset}m van waterlichaam"
    
    return lsm_lhm_snapped_gdf
