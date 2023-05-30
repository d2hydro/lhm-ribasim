import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from .utils import report_progress
from .lsm import snap_to_waterbodies
import warnings



def get_dm_nodes(dw_keys_df, district, mz_type ="d", out_type=int):
    df = dw_keys_df[dw_keys_df.oid == district]
    dm_nodes = [out_type(i) for i in df[df.kty == mz_type].nid.unique()]
    return dm_nodes


def find_unique_dm_links(
        lsw_nodes_gdf: gpd.GeoDataFrame,
        dw_keys_df: pd.DataFrame,
        mz_type ="d") -> list:
    """Finding unique DM nodes."""
    lsw_dm_links = []
    for dw, df in dw_keys_df.groupby(by=["oid"]):
        dm_nodes = df.loc[df.kty == mz_type].nid.unique()
        if len(dm_nodes) == 1:
            df = lsw_nodes_gdf[lsw_nodes_gdf["DWRN"] == dw[0]]
            for row in df.itertuples():
                lsw_dm_links += [(row.LSWFINAL, dm_nodes[0])]
    return lsw_dm_links


def find_routing_to_dm_links(
        lsw_nodes_gdf: gpd.GeoDataFrame,
        lsw_gdf: gpd.GeoDataFrame,
        dw_keys_df: pd.DataFrame,
        lkm_links_gdf: gpd.GeoDataFrame, 
        lsm_lhm_gdf: gpd.GeoDataFrame,
        lkm_waterlichamen_gdf: gpd.GeoDataFrame,
        ) -> list:

    # algemene functie om links te vinden mbv een LKM link
    def find_links(row, lsw_id):
        lsw_dm_links = [] 
        polygon = lsw_gdf.at[lsw_id, "geometry"]
        df = lsm_lhm_gdf[lsm_lhm_gdf.within(polygon)]# we kijken of er lateralen in het gebied van de LSW liggen
       
        if not df.empty:
            lsw_dm_links +=[(lsw_id, i) for i in df.DM.unique()]
       
        if row.nodeto.startswith("LSM"): # de link moet starten met LSM
            point = Point(row.geometry.bounds[2:]) # de Point van de nodeto
            waterlichaam_gdf = lkm_waterlichamen_gdf[lkm_waterlichamen_gdf.intersects(point)] # het waterlichaam waar punt op snapt
           
            if not waterlichaam_gdf.empty: # er moet wél een waterlichaam gevonden worden
                waterlichaam = waterlichaam_gdf.iloc[0] # dan pakken we het eerste waterlichaam (als het goed is is de lengte altijd 1)
                df = lsm_lhm_snapped_gdf[lsm_lhm_snapped_gdf.within(waterlichaam.geometry)] # we zoeken naar lsm_lhm laterale knopen binnen het waterlichaam
                df = lsm_lhm_snapped_gdf[lsm_lhm_snapped_gdf.within(waterlichaam.geometry)]
                
                if not df.empty: # we hebben gevonden!
                    lsw_dm_links = [(lsw_id, i) for i in df.DM.unique()] # hier maken we links van de lsw_id naar de unieke DM-knopen   
                    lsw_dm_links = [i for i in lsw_dm_links if i[1] in dm_nodes_stringified]
        
        return lsw_dm_links

    # set lsw_gdf index to LSWFINAL
    if lsw_gdf.index.name !="LSWFINAL":
        lsw_gdf.set_index("LSWFINAL", inplace=True)
    
    # snap lsm_lhm to water-bodies
    lsm_lhm_snapped_gdf = snap_to_waterbodies(lsm_lhm_gdf, lkm_waterlichamen_gdf, offset=250)


    all_links = []

    # iterate trough lsw_nodes and find
    for idx, row in enumerate(lsw_nodes_gdf.itertuples()):
        report_progress(idx, len(lsw_nodes_gdf))
        lsw_id = row.LSWFINAL
        district = row.DWRN
        dm_nodes = get_dm_nodes(dw_keys_df, district)
        dm_nodes_stringified = [str(i) for i in dm_nodes]
        lkm_links_iter = lkm_links_gdf[lkm_links_gdf.nodefrom == str(lsw_id)].itertuples() 
       
        for row in lkm_links_iter:
            links = []
            links = find_links(row, lsw_id) 
          
            if (not links):
                counter = 0
             
                while (not links) and (counter < 10) :
                    df = lkm_links_gdf[lkm_links_gdf.nodefrom == row.nodeto]
                 
                    if not df.empty:
                        row = df.iloc[0]
                        links = find_links(row, lsw_id)
                    counter += 1
            
            all_links += [i for i in links if i not in all_links]

    return all_links


def find_shortest_dm_links(
        lsw_nodes_gdf: gpd.GeoDataFrame,
        dw_keys_df: pd.DataFrame,
        dm_nodes_gdf: gpd.GeoDataFrame
        ):

    all_links = []
    for row in lsw_nodes_gdf.itertuples():
        lsw_id = row.LSWFINAL
        district = row.DWRN
        dm_nodes = get_dm_nodes(dw_keys_df, district)
        dm_nodes = [str(i) for i in dm_nodes]
    
        dm_node = dm_nodes_gdf.at[dm_nodes_gdf.loc[dm_nodes_gdf.ID.isin(dm_nodes)].distance(row.geometry).sort_values().index[0], "ID"]
    
        all_links += [(lsw_id, int(dm_node))]

    return all_links


def links_to_geodataframe(
        lsw_dm_links: list,
        lsw_nodes_gdf: gpd.GeoDataFrame,
        dm_nodes_gdf: gpd.GeoDataFrame
        ):
    """Convert lsw_dm_links list to a GeoDataFrame with LineStrings."""
    warnings.simplefilter(action='ignore', category=FutureWarning)
        
    lsw_dm_links_gdf = gpd.GeoDataFrame(lsw_dm_links, columns = ["node_from", "node_to"], geometry = gpd.GeoSeries(), crs=28992)
        
    def make_line_string(row):
        report_progress(row._name, len(lsw_dm_links_gdf))
        point_from = lsw_nodes_gdf.loc[row.node_from]
        point_to = dm_nodes_gdf.loc[dm_nodes_gdf.ID == str(row.node_to)].geometry

        return LineString([[point_from.x, point_from.y], [point_to.x, point_to.y]])
    
    lsw_dm_links_gdf.loc[:, "geometry"] = lsw_dm_links_gdf.apply((lambda x: make_line_string(x)), axis=1)

    lsw_dm_links_gdf["node_from"] = lsw_dm_links_gdf["node_from"].astype(str)
    lsw_dm_links_gdf["node_to"] = lsw_dm_links_gdf["node_to"].astype(str)
    
    return lsw_dm_links_gdf