import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString


def lsw_network(lsws_gdf:gpd.GeoDataFrame, lsw_routing_df: pd.DataFrame) -> gpd.GeoDataFrame:
    lsw_centroids_gdf = lsws_gdf.set_index("LSWFINAL")["geometry"].centroid
    lsw_routing_gdf = gpd.GeoDataFrame(
        lsw_routing_df,
        geometry=gpd.GeoSeries(),
        crs="epsg:28992"
        )
    
    def get_point(lsw_id):
        pnt = lsw_centroids_gdf[lsw_id]
        if isinstance(pnt, gpd.GeoSeries):
            return pnt.iat[0]
        else:
            return pnt
    
    lsw_routing_gdf["geometry"] = gpd.GeoSeries([LineString((get_point(x.lsw_from), get_point(x.lsw_to))) for x in lsw_routing_df.itertuples()])
    return lsw_routing_gdf