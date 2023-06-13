import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString
import numpy as np
from lhm.read import read_lsw_lad, read_lsw_vad


def lsw_network(lsw_gdf:gpd.GeoDataFrame, lsw_routing_df: pd.DataFrame, dissolve_lsws=False) -> gpd.GeoDataFrame:
    if dissolve_lsws:
        lsw_gdf = lsw_gdf.dissolve(by="LSWFINAL").reset_index()
    else:
        lsw_gdf = lsw_gdf.drop_duplicates("LSWFINAL")
    lsw_nodes_gdf = lsw_gdf.set_index("LSWFINAL")
    lsw_nodes_gdf["geometry"] = lsw_nodes_gdf["geometry"].centroid
    lsw_links_gdf = gpd.GeoDataFrame(
        lsw_routing_df,
        geometry=gpd.GeoSeries(),
        crs="epsg:28992"
        )
    
    def get_point(lsw_id):
        pnt = lsw_nodes_gdf.at[lsw_id, "geometry"]
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


def get_lsw_profiles_mozart(
        ladvalue_dik: str,
        vadvalue_dik: str,
        nodes_gdf: gpd.GeoDataFrame,
        lsw_nodes_gdf: gpd.GeoDataFrame
        ) -> pd.DataFrame:
    # %% op basis van level-area-value tabel
    lsw_lad_df = read_lsw_lad(ladvalue_dik).sort_values(
        by=["lsw", "level"])[["level", "lsw", "area"]].rename(columns={"lsw": "original_id"})
    
    lsw_lad_df = lsw_lad_df.loc[lsw_lad_df.original_id.isin(nodes_gdf.original_id)].reset_index(drop=True)
    
    lsw_lad_df["storage"] = np.NAN
    lsw_lad_df["remarks"] = "berekend uit ladvalue.dik"
    
    for _, df in lsw_lad_df.groupby(by="original_id"):
    
        # compute storage from level-area
        shifted_df = df.shift(1)
        delta_h = df["level"] - shifted_df["level"]
        delta_a = df["area"] - shifted_df["area"]
        storage = ((delta_h * shifted_df["area"]) + ((delta_a * delta_h) / 2)).cumsum()
        storage.iloc[0] = 0
    
        # add storage
        lsw_lad_df.loc[storage.index, "storage"] = storage
    
    # %%  op basis van volume-area-value tabel
    lsw_vad_df = read_lsw_vad(vadvalue_dik).sort_values(
        by=["lsw", "volume"])[["lsw", "volume", "area"]].rename(columns={"volume": "storage", "lsw": "original_id"})
    lsw_vad_df = lsw_vad_df.loc[~lsw_vad_df.original_id.isin(lsw_lad_df.original_id.unique())]
    lsw_vad_df = lsw_vad_df.loc[lsw_vad_df.original_id.isin(nodes_gdf.original_id)].reset_index(drop=True)
    lsw_vad_df["level"] = np.NAN
    lsw_vad_df["remarks"] = "berekend uit vadvalue.dik"
    
    for lsw, df in lsw_vad_df.groupby(by="original_id"):
        
        # compute delta_h from volume-area
        shifted_df = df.shift(1)
        delta_s = df["storage"] - shifted_df["storage"]
        delta_a = df["area"] - shifted_df["area"]
        delta_h = ((shifted_df["area"] / delta_s) + (delta_a / (2 * delta_s))) ** -1
        delta_h.iloc[0] = 0
        level = delta_h + lsw_nodes_gdf.set_index("LSWFINAL").at[lsw, "DEPTH_SF_W"]
        lsw_vad_df.loc[level.index, "level"] = level
    
    return pd.concat([lsw_lad_df, lsw_vad_df])
