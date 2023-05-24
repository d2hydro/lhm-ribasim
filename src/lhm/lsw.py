import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point


def lsw_network(lsw_gdf:gpd.GeoDataFrame, lsw_routing_df: pd.DataFrame) -> gpd.GeoDataFrame:
    lsw_nodes_gdf = lsw_gdf.set_index("LSWFINAL")["geometry"].centroid
    lsw_links_gdf = gpd.GeoDataFrame(
        lsw_routing_df,
        geometry=gpd.GeoSeries(),
        crs="epsg:28992"
        )
    
    def get_point(lsw_id):
        pnt = lsw_nodes_gdf[lsw_id]
        if isinstance(pnt, gpd.GeoSeries):
            return pnt.iat[0]
        else:
            return pnt
    
    lsw_links_gdf["geometry"] = gpd.GeoSeries([LineString((get_point(x.node_from), get_point(x.node_to))) for x in lsw_links_gdf.itertuples()])
    return lsw_links_gdf, lsw_nodes_gdf

def lsw_end_nodes(lsw_links_gdf:gpd.GeoDataFrame, lsw_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    end_nodes_gdf = lsw_gdf[~lsw_gdf.LSWFINAL.isin(lsw_links_gdf.node_from)]
    end_nodes_gdf.loc[:, "geometry"] = end_nodes_gdf.geometry.centroid
    return end_nodes_gdf