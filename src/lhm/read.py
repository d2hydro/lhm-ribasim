import pandas as pd
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point
import re
from typing import Literal


def read_dm_mz_to_obese(dm_mz_obese_file: str, flow_type: Literal["D", "E"] = None) -> pd.DataFrame:
    dm_mz_lsm_df = pd.read_csv(
        dm_mz_obese_file,
        header=None,
        quotechar="'",
        names=["DM", "DWRN", "flow_type", "node_type", 'LSM_id', "value_type", "value"],
        delim_whitespace=True)
    if flow_type is not None:
        dm_mz_lsm_df = dm_mz_lsm_df[dm_mz_lsm_df["flow_type"] == flow_type]
        
    # strip = from LSM_id
    dm_mz_lsm_df["LSM_id"] = dm_mz_lsm_df["LSM_id"].str[1:]

    # set DWRN to string
    dm_mz_lsm_df = dm_mz_lsm_df[~dm_mz_lsm_df["DWRN"].isna()]
    dm_mz_lsm_df["DWRN"] = dm_mz_lsm_df["DWRN"].astype(int)

    return dm_mz_lsm_df


def read_dw_keys(key_file:str) -> pd.DataFrame:
    pattern = r'KEY oid\s+(\d+)\s+kty\s+"(\w+)"\s+rid\s+(\d+)\s+nid\s+(\d+)\s+(?:ds\s+"(.*?)"\s+)?cp\s+"(.*?)"'

    with key_file.open() as file:
        lines = file.readlines()
        filtered_lines = [line.strip() for line in lines if not line.startswith("//")]
        file_content = "\n".join(filtered_lines)
        matches = re.findall(pattern, file_content, re.DOTALL)
    
    df = pd.DataFrame(matches, columns=['oid', 'kty', 'rid', 'nid', 'ds', 'cp'])
    df["oid"] = df["oid"].astype(int)
    df["nid"] = df["nid"].astype(str)
    df["rid"] = df["rid"].astype(int)
    df["node_to"] = None
    df["node_from"] = None
    mask = df["kty"] == "d"
    df.loc[mask, "node_to"] = df[mask]["nid"]
    mask = df["kty"] == "e"
    df.loc[mask, "node_from"] = df[mask]["nid"]
    return df


def read_dik_file(dik_file:str, columns:list) -> pd.DataFrame:
    return pd.read_csv(
        dik_file, 
        sep= " ",
        names=columns,
        skipinitialspace=True
        )

def read_lsw_routing(dik_file:str) -> pd.DataFrame:
    return read_dik_file(
        dik_file,
        columns=["node_from", "node_to", "direction", "fraction"]
        )

def read_lsw_lad(dik_file:str) -> pd.DataFrame:
    return read_dik_file(
        dik_file,
        columns=["level", "lsw", "area", "discharge"]
        )

def read_lsw_vad(dik_file:str) -> pd.DataFrame:
    return read_dik_file(
        dik_file,
        columns=["lsw", "volume", "area", "discharge"]
        )

def read_sobek_network_shps(shps_dir: str):
    shps_path = Path(shps_dir)
    network_l_gdf = gpd.read_file(shps_path / "NETWORK_l.shp")
    network_l_gdf.columns = [i.strip() for i in network_l_gdf.columns]
    network_l_gdf.rename(columns={"ID": "index", "ID_FROM": "node_from", "ID_TO": "node_to"}, inplace=True)
    network_l_gdf.set_index("index", drop=False, inplace=True)

    network_n_gdf = gpd.read_file(shps_path / "NETWORK_n.shp")
    network_n_gdf.columns = [i.strip() for i in network_n_gdf.columns]
    network_n_gdf.rename(columns={"ID": "index"}, inplace=True)
    network_n_gdf.set_index("index", drop=False, inplace=True)
    
    return network_l_gdf, network_n_gdf

def read_dm_network_shps(shps_dir: str):
    shps_path = Path(shps_dir)
    # DM-knopen en -links inlezen
    dm_links_gdf = gpd.read_file(shps_path / "DM_links.shp")
    dm_links_gdf.columns = [i.strip() for i in dm_links_gdf.columns]
    dm_links_gdf.rename(columns={"ID": "index", "ID_FROM": "node_from", "ID_TO": "node_to"}, inplace=True)
    dm_links_gdf.set_index("index", drop=False, inplace=True)

    # DM-knopen en -links inlezen
    dm_nodes_gdf = gpd.read_file(shps_path / "DM_nodes.shp")
    dm_nodes_gdf.columns = [i.strip() for i in dm_nodes_gdf.columns]
    dm_nodes_gdf.rename(columns={"ID": "index"}, inplace=True)
    dm_nodes_gdf.set_index("index", drop=False, inplace=True)
    
    return dm_links_gdf, dm_nodes_gdf

def read_lkm_network_links(lkm_links: str, lkm_only: bool = True):
    lkm25_links_gdf = gpd.read_file(lkm_links)
    if lkm_only:
        lkm25_links_gdf = lkm25_links_gdf.loc[~lkm25_links_gdf["nodefrom"].str.startswith("LSM")]

    lkm25_links_gdf["nodefrom"] = lkm25_links_gdf["nodefrom"].apply(lambda x: f"LKM_{x}")
    lkm25_links_gdf["nodeto"] = lkm25_links_gdf["nodeto"].apply(lambda x: f"LKM_{x}")
    lkm25_links_gdf.rename(columns={"nodeto": "node_to", "nodefrom": "node_from"}, inplace=True)
    lkm25_links_gdf.loc[:, ["index"]] = lkm25_links_gdf["linkid"].apply(lambda x: f"LKM_{x}")

    return lkm25_links_gdf

def read_lsm_lhm(lsm3_locations_csv:str, knoop_district_csv:str) -> gpd.GeoDataFrame:
    # read knoop_district_csv and extract DM-MZ type
    lsm_lhm_df = pd.read_csv(knoop_district_csv, sep=";").set_index("LSM3_ID")
    lsm_lhm_df[["DM", "MZ", "DMMZ_type"]] = lsm_lhm_df["DMMZ_ID"].str.extract(r'DM_\s+(\d+)_MZ_\s+(\d+)_type_(\w+)')
    #lsm_lhm_df["DM"] = lsm_lhm_df["DM"].astype(int)
    # read lsm locations csv and make spatial
    lsm3_locations_df = pd.read_csv(lsm3_locations_csv, sep=";", low_memory=False).set_index("FEWS_IDs")
    lsm3_locations_df = lsm3_locations_df.loc[lsm3_locations_df.index.isin(lsm_lhm_df.index)]
    lsm3_locations_df["geometry"] = lsm3_locations_df.apply(
        (lambda x: Point(x.Latitude, x.Longitude)),
        axis=1
        )
    lsm3_locations_gdf = gpd.GeoDataFrame(lsm3_locations_df, crs="epsg:28992")
    
    # join tables
    lsm_lhm_gdf = lsm3_locations_gdf.join(lsm_lhm_df)

    return lsm_lhm_gdf

def read_dm_nds(nds_file:str) -> pd.DataFrame:
    node_pattern = r"(?s)NODE(.*?)node"
    values_pattern = r'(?:rid\s+)?(\d+)(?:\s+id\s+(\d+))?(?:\s+nm\s+"(.*?)")?(?:\s+ty\s+(\d+))?(?:\s+ar\s+([\d.]+))?(?:\s+vo\s+([\d.]+))?(?:\s+s0\s+([\d.]+))?(?:\s+ws\s+(\d+))?'
    la_pattern = r"la tbl TBLE\n([\s\S]*?)\ntble"
    lv_pattern = r"lv tbl TBLE\n([\s\S]*?)\ntble"
    columns = ["rid","id", "nm","ty","ar","vo","s0","ws", "lav"]
  
    def level_area_storage(node_text):
        series = []
        # vinden la-tabel
        match = re.search(la_pattern, node_text)
        if match is not None:
            series = [series_from_table_text(
                match.group(1),
                "area")]
        match = re.search(lv_pattern, node_text)
        if match is not None:
            series += [series_from_table_text(
                match.group(1),
                "storage")]
        if series:
            return pd.concat(series, axis=1)
        else:
            return None
    
    def series_from_table_text(tbl_text, name="area"):
        lines = tbl_text.split('\n')
        lines = [line for line in lines if line.strip() != '']
        values = [line.split()[0:2] for line in lines]
        data = [i[1] for i in values]
        index = [i[0] for i in values]
        return pd.Series(data, index=index, name=name).astype(float) * 1000000
        
    def strip_node_text(node_match):
        node_text = node_match.strip()
        match = re.search(values_pattern, node_text)
        return [i for i in match.groups()] + [level_area_storage(node_text)]

    nds_file = Path(nds_file)
    text = nds_file.read_text()
    cleaned_text = re.sub(r'^//.*?$', '', text, flags=re.MULTILINE)
        
    node_matches = re.findall(node_pattern, cleaned_text)
    data = [strip_node_text(i) for i in node_matches]
    
    df = pd.DataFrame(data, columns=columns)
    df["ar"] = df["ar"].astype(float)
    df["vo"] = df["vo"].astype(float)

    return df