from config import LHM_DIR, DATA_DIR, MOZART_DIR, load_src
import geopandas as gpd
import numpy as np
import pandas as pd

load_src()

from lhm.read import read_lsw_lad, read_lsw_vad, read_dm_nds

#%%
lhm_topology_gpkg = DATA_DIR / "lhm_topologie.gpkg"
lsw_nodes_gdf = gpd.read_file(lhm_topology_gpkg, layer="lsw-nodes")

# Toevoegen LSW basin-knopen en -profielen
nodes_gdf = lsw_nodes_gdf.sort_values(
    by="LSWFINAL"
    )[
      ["LSWFINAL","geometry"]
      ].rename(
          columns={"LSWFINAL":"original_id"}
          )
nodes_gdf["origin"] = nodes_gdf["original_id"].apply(lambda x: f"MZlsw_{x}")


# %%
dm_nodes_gdf = gpd.read_file(lhm_topology_gpkg, layer="dm-nodes")

dm_nodes_gdf = dm_nodes_gdf.sort_values(by="ID")[["ID", "geometry"]].rename(
    columns={"ID":"original_id"}
    )
dm_nodes_gdf["origin"] = dm_nodes_gdf["original_id"].apply(lambda x: f"DMnd_{x}")

nodes_gdf = pd.concat([nodes_gdf, dm_nodes_gdf])
nodes_gdf["type"] = "Basin"

# Bouwen RIBASIM basin-profielen

# %% op basis van level-area-value tabel
lsw_lad_df = read_lsw_lad(MOZART_DIR / r"mozartin/ladvalue.dik").sort_values(
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
lsw_vad_df = read_lsw_vad(MOZART_DIR / r"mozartin/vadvalue.dik").sort_values(
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


# %% uit DM nds file
from lhm.read import read_lsw_lad, read_lsw_vad, read_dm_nds
def default_profile():
    return pd.DataFrame(
        data = [[-5, 0 ,0, None], [5, 1000000 , 10000000, None]],
        columns= ["level", "area", "storage", "remarks"]
        )


def lav(row):
    if row.original_id not in dm_nds_df.index:
        profile = default_profile()
        profile["remarks"] = "default-profile"
    else:
        node_row = dm_nds_df.loc[row.original_id]
        if node_row.lav is None:
            profile = default_profile()
            remarks = []
            if not ((node_row.ar == 0.) or (node_row.ar is None)):
                profile["area"] = node_row.ar
                remarks += ["constant area" ]
            if not ((node_row.vo == 0.) or (node_row.vo is None)):
                profile["storage"] = node_row.vo
                remarks += ["constant volume"]
            if remarks:
                profile["remarks"] = ",".join(remarks)
            else:
                profile["remarks"] = "default-profile"
        else:
            profile = node_row.lav.copy(deep=True)
            profile.index.name= "level"
            profile.reset_index(inplace=True)
            profile["remarks"] = "uit dm nds.txt"
    profile["original_id"] = row.original_id
    return profile

dm_nds_df = read_dm_nds(LHM_DIR / r"dm/txtfiles_git/nds.txt").set_index("id")
dm_profiles_df = pd.concat([lav(row) for row in dm_nodes_gdf.itertuples()])

basin_profile_df = gpd.GeoDataFrame(pd.concat([lsw_lad_df, lsw_vad_df, dm_profiles_df]), geometry=gpd.GeoSeries(), crs=28992)

# %%schrijven van bestanden

ribasim_topology_gpkg = DATA_DIR / "ribasim_model.gpkg"

nodes_gdf.to_file(ribasim_topology_gpkg, layer="Node")
basin_profile_df.to_file(ribasim_topology_gpkg, layer="Basin / profile")
