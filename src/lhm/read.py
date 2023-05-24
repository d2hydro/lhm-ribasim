import pandas as pd
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point
import re


def read_dw_keys(key_file:str) -> pd.DataFrame:
    key_file = Path(key_file)
    pattern = r'KEY oid\s+(\d+)\s+kty\s+"(\w+)"\s+rid\s+(\d+)\s+nid\s+(\d+)\s+ds\s+"(.*?)"\s+cp\s+"(.*?)"'
    matches = re.findall(pattern, key_file.read_text(), re.DOTALL)
    df = pd.DataFrame(matches, columns=['oid', 'kty', 'rid', 'nid', 'ds', 'cp'])
    df["oid"] = df["oid"].astype(int)
    df["nid"] = df["nid"].astype(int)
    df["rid"] = df["rid"].astype(int)
    df["node_to"] = None
    df["node_from"] = None
    mask = df["kty"] == "d"
    df.loc[mask, "node_to"] = df[mask]["nid"]
    mask = df["kty"] == "e"
    df.loc[mask, "node_from"] = df[mask]["nid"]
    return df

def read_lsw_routing(dik_file:str) -> pd.DataFrame:
    dik_file = Path(dik_file)
    return pd.read_csv(
        dik_file, 
        sep= " ",
        names=["node_from", "node_to", "direction", "fraction"]
        )


def read_lsm_lhm(lsm3_locations_csv:str, knoop_district_csv:str) -> gpd.GeoDataFrame:
    lsm_lhm_df = pd.read_csv(knoop_district_csv, sep=";").set_index("LSM3_ID")
    lsm_lhm_df[["DM", "MZ", "type"]] = lsm_lhm_df["DMMZ_ID"].str.extract(r'DM_\s+(\d+)_MZ_\s+(\d+)_type_(\w+)')
    lsm3_locations_df = pd.read_csv(lsm3_locations_csv, sep=";").set_index("FEWS_IDs")
    
    lsm3_locations_df = lsm3_locations_df.loc[lsm3_locations_df.index.isin(lsm_lhm_df.index)]
    
    # make spatial
    lsm3_locations_df["geometry"] = lsm3_locations_df.apply(
        (lambda x: Point(x.Latitude, x.Longitude)),
        axis=1
        )
    lsm3_locations_gdf = gpd.GeoDataFrame(lsm3_locations_df, crs="epsg:28992")
    
    
    lsm_lhm_gdf = lsm3_locations_gdf.join(lsm_lhm_df)
    lsm_lhm_gdf[["DM", "MZ", "type"]] = lsm_lhm_gdf["DMMZ_ID"].str.extract(r'DM_\s+(\d+)_MZ_\s+(\d+)_type_(\w+)')
    return lsm_lhm_gdf
