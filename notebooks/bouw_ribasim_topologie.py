from config import LHM_DIR, DATA_DIR, load_src
import geopandas as gpd
import pandas as pd
import xarray as xr
import numpy as np

load_src()

from lhm.read import read_dm_nds


model_name="ribasim_model_flevoland"
mask_poly = gpd.read_file(DATA_DIR / "mask.gpkg").iloc[0].geometry
bbox = mask_poly.bounds
#bbox = None
lhm_topology_gpkg = DATA_DIR / "lhm_topologie.gpkg"


def new_index(arr):
    sorted_arr = np.sort(arr)
    unique_values = np.unique(sorted_arr)
    max_value = unique_values.max()
    lowest_non_used_integer = next((i for i in range(1, max_value + 2) if i not in unique_values), None)
    return lowest_non_used_integer


#% Inlezen LSW nodes
lsw_nodes_gdf = gpd.read_file(lhm_topology_gpkg, layer="lsw-nodes", bbox=bbox)

# Toevoegen LSW basin-knopen en -profielen
nodes_gdf = lsw_nodes_gdf.sort_values(
    by="LSWFINAL"
    )[
      ["LSWFINAL","geometry"]
      ].rename(
          columns={"LSWFINAL":"lhm_id"}
          )
nodes_gdf["origin"] = nodes_gdf["lhm_id"].apply(lambda x: f"MZlsw_{x}")

# % Sanitizen van LSW nodes tot beschikbare invoer vanuit LSWs
input_mozart_ds = xr.open_dataset(DATA_DIR / r"ribasim_testmodel/simplified_SAQh.nc")
nodes_gdf = nodes_gdf.loc[
    nodes_gdf["lhm_id"].isin(input_mozart_ds["node"].values.astype(int))
    ]

# % Inlezen DM-nodes
dm_nodes_gdf = gpd.read_file(lhm_topology_gpkg, layer="dm-nodes", bbox=bbox)

dm_nodes_gdf = dm_nodes_gdf.sort_values(by="ID")[["ID", "geometry"]].rename(
    columns={"ID":"lhm_id"}
    )
dm_nodes_gdf["origin"] = dm_nodes_gdf["lhm_id"].apply(lambda x: f"DMnd_{x}")

nodes_gdf = pd.concat([nodes_gdf, dm_nodes_gdf])
nodes_gdf["type"] = "Basin"

# % Bouwen RIBASIM basin-profielen
da = input_mozart_ds.profile.transpose("node", "profile_col", "profile_row")
ribasim_profiles_gdf = pd.DataFrame(
        [item.T for sublist in da.values for item in sublist.T],
        columns = ["storage", "area","discharge", "level"]
        )
ribasim_profiles_gdf["origin"] = [f"MZlsw_{str(int(x))}" for x in da.node.values for _ in range(4)]
ribasim_profiles_gdf["remarks"] = "uit simplified_SAQh.nc"


# % toevoegen RIBASIM-id aan nieuw knopen
ribasim_nodes_gdf = gpd.read_file(
    DATA_DIR / r"ribasim_testmodel/model.gpkg",
    layer='Node',
    fid_as_index=True,
    bbox=bbox
    )
ribasim_basins_filter = ribasim_nodes_gdf['type'] == 'Basin'
nodes_mozart_filter = nodes_gdf["origin"].str.startswith("MZlsw_")

ribasim_nodes_original_id_gdf = gpd.sjoin_nearest(
    ribasim_nodes_gdf[ribasim_basins_filter],
    nodes_gdf[nodes_mozart_filter],
    how='left').reset_index().set_index("lhm_id")


nodes_gdf.loc[nodes_mozart_filter, ["ribasim_id"]] = nodes_gdf.loc[nodes_mozart_filter, "lhm_id"].apply(lambda x: ribasim_nodes_original_id_gdf.at[x, "fid"])
nodes_gdf.reset_index(inplace=True, drop=True)
nodes_gdf["lhm_id"] = nodes_gdf["lhm_id"].astype(str)


# % uit DM nds file
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
    profile["origin"] = f"DMnd_{row.lhm_id}"
    return profile

dm_nds_df = read_dm_nds(LHM_DIR / r"dm/txtfiles_git/nds.txt").set_index("id")
dm_profiles_df = pd.concat([lav(row) for row in dm_nodes_gdf.itertuples()])

basin_profile_df = gpd.GeoDataFrame(pd.concat([ribasim_profiles_gdf, dm_profiles_df]), geometry=gpd.GeoSeries(), crs=28992)

#% inlezen links
from shapely.geometry import Point, LineString
def get_flow_point(geometries, node_type):
    
    if len(geometries) > 1:
        point = geometries.iloc[0].boundary.geoms[0]
        point = Point(point.x, point.y - 100)
    elif node_type == "TabulatedRatingCurve":
        line = geometries.iloc[0]
        point = line.interpolate(min(100, line.length / 2))
    else:
        line = geometries.iloc[0]
        point = line.centroid
    return point



dm_links_gdf = gpd.read_file(lhm_topology_gpkg, layer="dm-links", bbox=bbox).rename(
    columns={"ID_FROM": "node_from", "ID_TO": "node_to"}
    )
dm_links_gdf["node_from"] = dm_links_gdf["node_from"].apply(lambda x: f"DMnd_{str(x)}")
dm_links_gdf["node_to"] = dm_links_gdf["node_to"].apply(lambda x: f"DMnd_{str(x)}")

lsw_dm_links_gdf = gpd.read_file(lhm_topology_gpkg, layer="lsw-dm-links", bbox=bbox)
lsw_dm_links_gdf["node_from"] = lsw_dm_links_gdf["node_from"].apply(lambda x: f"MZlsw_{str(x)}")
lsw_dm_links_gdf["node_to"] = lsw_dm_links_gdf["node_to"].apply(lambda x: f"DMnd_{str(x)}")

lsw_links_gdf = gpd.read_file(lhm_topology_gpkg, layer="lsw-links", bbox=bbox)
lsw_links_gdf["node_from"] = lsw_links_gdf["node_from"].apply(lambda x: f"MZlsw_{str(x)}")
lsw_links_gdf["node_to"] = lsw_links_gdf["node_to"].apply(lambda x: f"MZlsw_{str(x)}")


links_gdf = pd.concat([dm_links_gdf, lsw_dm_links_gdf, lsw_links_gdf])
links_mask = links_gdf.node_from.isin(nodes_gdf.origin) & links_gdf.node_to.isin(nodes_gdf.origin)
links_gdf = links_gdf.loc[links_mask]

nodes_mask = nodes_gdf.origin.isin(links_gdf.node_to) | nodes_gdf.origin.isin(links_gdf.node_from)
nodes_gdf = nodes_gdf.loc[nodes_mask]
nodes_gdf.index = nodes_gdf.reset_index(drop=True).index.values + 1

passed_indexes = []
tabulated_profiles_df = pd.DataFrame(columns=["lhm_id", "level", "discharge", "remarks", "node_id"])
nodes = []
edges = []
manning_resistance = []
fractional_flow = []
_nodes_gdf = nodes_gdf.reset_index().set_index("origin")
lhm_indices = {v:k for k,v in nodes_gdf["origin"].to_dict().items()}
#%% we add 1 node 100m (or half-way the length) from the upstream node
for lhm_id, df in links_gdf.groupby("node_from"):
#lhm_id, df = list(links_gdf.groupby("node_from"))[0]
    if lhm_id in _nodes_gdf.index:
        print(f"links vanaf {lhm_id}")
    
    
        # we get the node to use it's properties
        node_from = _nodes_gdf.loc[lhm_id]
    
        # if there is a Q(H) curve we add that node
        if lhm_id in ribasim_profiles_gdf.origin.values:
            
            # we make sure we get a new, available index.
            tr_index = new_index(list(lhm_indices.values()))
            lhm_indices[f"tr_{tr_index}"] = tr_index
            
            # we add the node
            node_type = "TabulatedRatingCurve"
            tr_point = get_flow_point(df.geometry, node_type)
            nodes += [(tr_index, lhm_id, node_type, tr_point)]
    
            # we add the profile
            prof_df = ribasim_profiles_gdf[ribasim_profiles_gdf.origin == lhm_id][["level","discharge"]]
            prof_df["lhm_id"] = lhm_id
            prof_df["node_id"] = int(tr_index)
            tabulated_profiles_df = pd.concat([tabulated_profiles_df, prof_df])
            # we define a edge between the upstream node and 
            edges += [(node_from["index"], tr_index, LineString([node_from.geometry, tr_point]))]
        else: # we don't need an extra node
            node_type = "ManningResistance"
        # else:
        #     node_type = "ManningResistance"
        #     manning_resistance = [(flow_index, 1000,0.04,50,2)]
    
        # define one point 100m downstream, max 1/2 link length
    
        # add fraction or manning nodes
        if len(df) > 1:
            fraction = 1 / len(df)
        for row in df.itertuples():
            # we get, or make a node_to index
            node_to = _nodes_gdf.loc[row.node_to]
            to_index = node_to["index"]
    
            if node_type == "TabulatedRatingCurve":
                if len(df) > 1:
                    frac_point = row.geometry.centroid
                    frac_index = new_index(list(lhm_indices.values()))
                    lhm_indices[f"frac_{frac_index}"] = frac_index
                    nodes += [(frac_index, lhm_id, "FractionalFlow", frac_point)]
                    fractional_flow += [(frac_index, fraction)]
                    edges += [
                        (tr_index, frac_index, LineString([tr_point, frac_point])),
                        (frac_index, to_index, LineString([frac_point, node_to.geometry]))
                        ]
                else:
                    edges += [(tr_index, to_index, LineString([tr_point, node_to.geometry]))]
            else:
                manning_point = row.geometry.centroid
                manning_index = new_index(list(lhm_indices.values()))
                lhm_indices[f"manning_{manning_index}"] = manning_index
                nodes += [(manning_index, lhm_id, "ManningResistance", manning_point)]
                manning_resistance += [(manning_index, 1000,0.04,50,2)]
                edges += [
                    (node_from["index"], manning_index, LineString([node_from.geometry, manning_point])),
                    (manning_index, to_index, LineString([manning_point, node_to.geometry]))
                    ]
                

tabulated_profiles_df["remarks"] = "uit simplified_SAQh.nc"        

edges_gdf = gpd.GeoDataFrame(edges, columns=["from_node_id","to_node_id","geometry"], crs=28992)
edges_gdf["edge_type"] = "flow"
nodes_gdf = pd.concat(
    [nodes_gdf,
     gpd.GeoDataFrame(
         nodes,
         columns=["index","origin","type","geometry"],
         crs=28992
         ).set_index("index")]
    )

dm_nodes_gdf = nodes_gdf[nodes_gdf.origin.str.startswith("DMnd_")]
# %% vinden LevelBoundaries

level_boundary_mask  = nodes_gdf.index.isin(edges_gdf.to_node_id) & ~nodes_gdf.index.isin(edges_gdf.from_node_id)
nodes_gdf.loc[level_boundary_mask, ["type"]] = "LevelBoundary"
level_boundary_df = nodes_gdf.loc[nodes_gdf["type"] == "LevelBoundary"]
level_boundary_df["level"] = -0.01
level_boundary_df["node_id"] = level_boundary_df.index
level_boundary_df = level_boundary_df[["node_id", "level"]]

# # %% vinden FlowBoundaries
flow_boundary_mask  = ~nodes_gdf.index.isin(edges_gdf.to_node_id) & nodes_gdf.index.isin(edges_gdf.from_node_id) & ~nodes_gdf.origin.str.startswith("MZlsw")
nodes_gdf.loc[flow_boundary_mask, ["type"]] = "FlowBoundary"
flow_boundary_df = nodes_gdf.loc[nodes_gdf["type"] == "FlowBoundary"]
flow_boundary_df["flow_rate"] = 0.01
flow_boundary_df["node_id"] = flow_boundary_df.index
flow_boundary_df = flow_boundary_df[["node_id", "flow_rate"]]

# %%schrijven van bestanden
import ribasim
ribasim_node = ribasim.Node(static=nodes_gdf)
ribasim_edge = ribasim.Edge(static=edges_gdf)

# 2 mm/d precipitation, 1 mm/d evaporation
seconds_in_day = 24 * 3600
precipitation = 0.002 / seconds_in_day
evaporation = 0.001 / seconds_in_day

static_df = pd.DataFrame(nodes_gdf[nodes_gdf["type"] == "Basin"].reset_index()["index"].values, columns=["node_id"])
static_df["drainage"] = 0.0
static_df["potential_evaporation"] = evaporation
static_df["infiltration"] = 0.0
static_df["precipitation"] = precipitation
static_df["urban_runoff"] = 0.0

basin_profile_columns = ["origin", "level", "area", "remarks"]
basin_profile_df["level"] = basin_profile_df["level"].astype(float)
basin_profile_df = basin_profile_df[basin_profile_columns]
basin_idx = nodes_gdf.loc[nodes_gdf["type"] == "Basin"].origin.reset_index().set_index("origin")
basin_profile_df = basin_profile_df.loc[basin_profile_df.origin.isin(basin_idx.index)]
basin_profile_df["node_id"] = basin_profile_df["origin"].apply(lambda x: basin_idx.loc[x])
ribasim_basin = ribasim.Basin(
    profile=basin_profile_df,
    static=static_df
        )
ribasim_rating_curve = ribasim.TabulatedRatingCurve(static=tabulated_profiles_df)
ribasim_fractional_flow = ribasim.FractionalFlow(
    static=pd.DataFrame(fractional_flow, columns=["node_id", "fraction"])
    )
ribasim_manning_resistance = ribasim.ManningResistance(
    static=pd.DataFrame(manning_resistance,columns=["node_id","length","manning_n","profile_width","profile_slope"])
    )
if flow_boundary_df.empty:
    ribasim_flow_boundary = None
else:
    ribasim_flow_boundary = ribasim.FlowBoundary(static=flow_boundary_df)
ribasim_level_boundary = ribasim.LevelBoundary(static=level_boundary_df)

model = ribasim.Model(
    modelname=model_name,
    node=ribasim_node,
    edge=ribasim_edge,
    basin=ribasim_basin,
    level_boundary=ribasim_level_boundary,
    flow_boundary=ribasim_flow_boundary,
    manning_resistance=ribasim_manning_resistance,
    tabulated_rating_curve=ribasim_rating_curve,
    fractional_flow=ribasim_fractional_flow,
    starttime="2020-01-01 00:00:00",
    endtime="2021-01-01 00:00:00",
)

model.write(DATA_DIR / model_name)