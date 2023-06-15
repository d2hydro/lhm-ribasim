from config import LHM_DIR, DATA_DIR, load_src
import geopandas as gpd
import pandas as pd
import xarray as xr
import numpy as np

load_src()

from lhm.read import read_dm_nds

lhm_topology_gpkg = DATA_DIR / "lhm_topologie.gpkg"

def new_index(arr):
    sorted_arr = np.sort(arr)
    unique_values = np.unique(sorted_arr)
    max_value = unique_values.max()
    lowest_non_used_integer = next((i for i in range(1, max_value + 2) if i not in unique_values), None)
    return lowest_non_used_integer


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
ribasim_profiles_gdf["origin"] = [f"MZlsw_{str(int(x))}" for x in da.node.values for _ in range(4)]
ribasim_profiles_gdf["remarks"] = "uit simplified_SAQh.nc"


# %% toevoegen RIBASIM-id aan nieuw knopen
ribasim_nodes_gdf = gpd.read_file(
    DATA_DIR / r"ribasim_testmodel/model.gpkg",
    layer='Node',
    fid_as_index=True
    )
ribasim_basins_filter = ribasim_nodes_gdf['type'] == 'Basin'
nodes_mozart_filter = nodes_gdf["origin"].str.startswith("MZlsw_")

ribasim_nodes_original_id_gdf = gpd.sjoin_nearest(
    ribasim_nodes_gdf[ribasim_basins_filter],
    nodes_gdf[nodes_mozart_filter],
    how='left').reset_index().set_index("lhm_id")


nodes_gdf.loc[nodes_mozart_filter, ["ribasim_id"]] = nodes_gdf.loc[nodes_mozart_filter, "lhm_id"].apply(lambda x: ribasim_nodes_original_id_gdf.at[x, "fid"])
nodes_gdf.reset_index(inplace=True, drop=True)
nodes_gdf.index = nodes_gdf.index.values + 1
nodes_gdf["lhm_id"] = nodes_gdf["lhm_id"].astype(str)


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
    profile["lhm_id"] = f"DMnd_{row.lhm_id}"
    return profile

dm_nds_df = read_dm_nds(LHM_DIR / r"dm/txtfiles_git/nds.txt").set_index("id")
dm_profiles_df = pd.concat([lav(row) for row in dm_nodes_gdf.itertuples()])

basin_profile_df = gpd.GeoDataFrame(pd.concat([ribasim_profiles_gdf, dm_profiles_df]), geometry=gpd.GeoSeries(), crs=28992)

#%% inlezen links
from shapely.geometry import Point, LineString
def get_flow_point(geometries):
    points = geometries.apply(lambda x:x.interpolate(min(100, x.length / 2)))
    return Point(points.x.mean(), points.y.mean())


#%%
dm_links_gdf = gpd.read_file(lhm_topology_gpkg, layer="dm-links").rename(
    columns={"ID_FROM": "node_from", "ID_TO": "node_to"}
    )
dm_links_gdf["node_from"] = dm_links_gdf["node_from"].apply(lambda x: f"DMnd_{str(x)}")
dm_links_gdf["node_to"] = dm_links_gdf["node_to"].apply(lambda x: f"DMnd_{str(x)}")

lsw_dm_links_gdf = gpd.read_file(lhm_topology_gpkg, layer="lsw-links")
lsw_dm_links_gdf["node_from"] = lsw_dm_links_gdf["node_from"].apply(lambda x: f"MZlsw_{str(x)}")
lsw_dm_links_gdf["node_to"] = lsw_dm_links_gdf["node_to"].apply(lambda x: f"DMnd_{str(x)}")

lsw_links_gdf = gpd.read_file(lhm_topology_gpkg, layer="lsw-links")
lsw_links_gdf["node_from"] = lsw_links_gdf["node_from"].apply(lambda x: f"MZlsw_{str(x)}")
lsw_links_gdf["node_to"] = lsw_links_gdf["node_to"].apply(lambda x: f"MZlsw_{str(x)}")


links_gdf = pd.concat([dm_links_gdf, lsw_dm_links_gdf, lsw_links_gdf])



passed_indexes = []
tabulated_profiles_df = pd.DataFrame(columns=["lhm_id", "level", "discharge", "remarks"])
nodes = []
edges = []
manning_resistance = []
nodes_indices = nodes_gdf.index.values
_nodes_gdf = nodes_gdf.reset_index().set_index("origin")
# we add 1 node 100m (or half-way the length) from the upstream node
for lhm_id, df in links_gdf.groupby("node_from"):
    if lhm_id in _nodes_gdf.index:
        print(f"links vanaf {lhm_id}")
        # we admin the indices
        index = new_index(nodes_indices)
        nodes_indices = np.append(nodes_indices, index)
    
        # we get the node to use it's properties
        node = _nodes_gdf.loc[lhm_id]
        
        # define one point 100m downstream, max 1/2 link length
        point = get_flow_point(df.geometry)
    
        # we define a edge between the upstream node and 
        edges += [(node["index"], index, LineString([node.geometry, point]))]
    
        # if there is a Q(H) curve we add the node as such (and add the node to nodes)
        if lhm_id in ribasim_profiles_gdf.origin.values:
            df = ribasim_profiles_gdf[ribasim_profiles_gdf.origin == lhm_id][["level","discharge"]]
            df["lhm_id"] = lhm_id
            tabulated_profiles_df = pd.concat([tabulated_profiles_df, df])
            nodes += [(index, lhm_id, "TabulatedRatingCurve", point)]
        else:
            nodes += [(index, lhm_id, "ManningResistance", point)]
            manning_resistance = [(index, 1000,0.04,50,2)]

tabulated_profiles_df["remarks"] = "uit simplified_SAQh.nc" 
manning_resistance_df = gpd.GeoDataFrame(
    manning_resistance,
    columns=["node_id", "length", "manning_n", "profile_width", "profile_slope"],
    geometry=gpd.GeoSeries(),
    crs=28992)          

edges_gdf = gpd.GeoDataFrame(edges, columns=["from_node_id","to_node_id","geometry"], crs=28992)
nodes_gdf = pd.concat(
    [nodes_gdf,
     gpd.GeoDataFrame(
         nodes,
         columns=["index","origin","type","geometry"],
         crs=28992
         ).set_index("index")]
    )
# %%schrijven van bestanden

ribasim_topology_gpkg = DATA_DIR / "ribasim_model.gpkg"

nodes_gdf.to_file(ribasim_topology_gpkg, layer="Node")
edges_gdf.to_file(ribasim_topology_gpkg, layer="Edge")
basin_profile_columns = ["lhm_id", "level", "area", "storage", "remarks", "geometry"]

basin_profile_df[basin_profile_columns].to_file(ribasim_topology_gpkg, layer="Basin / profile")
gpd.GeoDataFrame(tabulated_profiles_df, geometry=gpd.GeoSeries(), crs=28992).to_file(
    ribasim_topology_gpkg,
    layer="TabulatedRatingCurve / static"
    )
gpd.GeoDataFrame(
    manning_resistance,
    columns=["node_id", "length", "manning_n", "profile_width", "profile_slope"],
    geometry=gpd.GeoSeries(),
    crs=28992).to_file(
        ribasim_topology_gpkg,
        layer="ManningResistance / static"
        )

