from config import LHM_DIR, DATA_DIR, load_src
import geopandas as gpd
import pandas as pd
import xarray as xr

load_src()

from lhm.read import read_dm_nds

lhm_topology_gpkg = DATA_DIR / "lhm_topologie.gpkg"

#%% Inlezen LSW nodes
lsw_nodes_gdf = gpd.read_file(lhm_topology_gpkg, layer="lsw-nodes")

# Toevoegen LSW basin-knopen en -profielen
nodes_gdf = lsw_nodes_gdf.sort_values(
    by="LSWFINAL"
    )[
      ["LSWFINAL","geometry"]
      ].rename(
          columns={"LSWFINAL":"lhm_id"}
          )
nodes_gdf["origin"] = nodes_gdf["lhm_id"].apply(lambda x: f"MZlsw_{x}")

# %% Sanitizen van LSW nodes tot beschikbare invoer vanuit LSWs
input_mozart_ds = xr.open_dataset(DATA_DIR / r"ribasim_testmodel/simplified_SAQh.nc")
nodes_gdf = nodes_gdf.loc[
    nodes_gdf["lhm_id"].isin(input_mozart_ds["node"].values.astype(int))
    ]

# %% Inlezen DM-nodes
dm_nodes_gdf = gpd.read_file(lhm_topology_gpkg, layer="dm-nodes")

dm_nodes_gdf = dm_nodes_gdf.sort_values(by="ID")[["ID", "geometry"]].rename(
    columns={"ID":"lhm_id"}
    )
dm_nodes_gdf["origin"] = dm_nodes_gdf["lhm_id"].apply(lambda x: f"DMnd_{x}")

nodes_gdf = pd.concat([nodes_gdf, dm_nodes_gdf])
nodes_gdf["type"] = "Basin"

# %% Bouwen RIBASIM basin-profielen
da = input_mozart_ds.profile.transpose("node", "profile_col", "profile_row")
ribasim_profiles_gdf = pd.DataFrame(
        [item.T for sublist in da.values for item in sublist.T],
        columns = ["storage", "area","discharge", "level"]
        )
ribasim_profiles_gdf["lhm_id"] = [str(int(x)) for x in da.node.values for _ in range(4)]
ribasim_profiles_gdf["remarks"] = "uit simplified_SAQh.nc"


# %% toevoegen RIBASIM-id aan nieuw knopen
ribasim_nodes_gdf = gpd.read_file(
    DATA_DIR / r"ribasim_testmodel/model.gpkg",
    layer='Node',
    engine='pyogrio', # Take pyogrio engine instead of fiona
    fid_as_index=True
    )
ribasim_basins_filter = ribasim_nodes_gdf['type'] == 'Basin'
nodes_mozart_filter = nodes_gdf["origin"].str.startswith("MZlsw_")

ribasim_nodes_original_id_gdf = gpd.sjoin_nearest(
    ribasim_nodes_gdf[ribasim_basins_filter],
    nodes_gdf[nodes_mozart_filter],
    how='left').reset_index().set_index("lhm_id")


nodes_gdf.loc[nodes_mozart_filter, ["ribasim_id"]] = nodes_gdf.loc[nodes_mozart_filter, "lhm_id"].apply(lambda x: ribasim_nodes_original_id_gdf.at[x, "fid"])

# %% uit DM nds file
def default_profile():
    return pd.DataFrame(
        data = [[-5, 0 ,0, None], [5, 1000000 , 10000000, None]],
        columns= ["level", "area", "storage", "remarks"]
        )


def lav(row):
    if row.lhm_id not in dm_nds_df.index:
        profile = default_profile()
        profile["remarks"] = "default-profile"
    else:
        node_row = dm_nds_df.loc[row.lhm_id]
        if node_row.lav is None:
            profile = default_profile()
            remarks = []
            if not (node_row.ar == 0.):
                if not pd.isna(node_row.ar):
                    profile["area"] = node_row.ar
                    remarks += ["constant area" ]
            if not (node_row.vo == 0.):
                if not pd.isna(node_row.vo):
                    print(node_row.vo)
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
    profile["lhm_id"] = row.lhm_id
    return profile

dm_nds_df = read_dm_nds(LHM_DIR / r"dm/txtfiles_git/nds.txt").set_index("id")
dm_profiles_df = pd.concat([lav(row) for row in dm_nodes_gdf.itertuples()])

basin_profile_df = gpd.GeoDataFrame(pd.concat([ribasim_profiles_gdf, dm_profiles_df]), geometry=gpd.GeoSeries(), crs=28992)

# %%schrijven van bestanden

ribasim_topology_gpkg = DATA_DIR / "ribasim_model.gpkg"

nodes_gdf.to_file(ribasim_topology_gpkg, layer="Node")
basin_profile_columns = ["lhm_id", "level", "area", "storage", "remarks", "geometry"]

basin_profile_df[basin_profile_columns].to_file(ribasim_topology_gpkg, layer="Basin / profile")
