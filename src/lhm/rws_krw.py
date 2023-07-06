from owslib.wfs import WebFeatureService
from dataclasses import dataclass
import geopandas as gpd
import networkx as nx

@dataclass
class KRW():
    url: str = r"https://geoservices.rijkswaterstaat.nl/apps/geoserver/kaderrichtlijn_water/ows?service=WFS"
    version: str = "2.0.0"
    wfs: WebFeatureService = None
    
    def __post_init__(self):
        self.wfs = WebFeatureService(url=self.url, version=self.version)  
        
    def get_layers(self, filter: str = None) -> list:
        layers = list(self.wfs.contents.keys())
        if filter is not None:
            layers =  [i for i in layers if filter.lower() in i.lower()]
        return layers
    
    def get_layer(self, layer):
        if layer in self.get_layers():
            response = self.wfs.getfeature(
                typename="kaderrichtlijn_water:KRW_oppervlaktewaterdelen_lijn",
                outputFormat='application/json')
            return gpd.read_file(response)

#%%
krw = KRW()
gdf = krw.get_layer("kaderrichtlijn_water:KRW_oppervlaktewaterlichamen_lijn")
gdf.set_index("id", inplace=True, drop=False)
gdf = gdf.explode(index_parts=True)
gdf["link_id"] = [f"{i[0]}_{i[1]}" for i in gdf.index]
gdf.reset_index(drop=True, inplace=True)
links_gdf = gdf.copy()


gdf["point_from"] = gdf.geometry.apply(lambda x: x.boundary.geoms[0])
gdf["point_to"] = gdf.geometry.apply(lambda x: x.boundary.geoms[-1])
points = list(set(gdf["point_from"].to_list() + gdf["point_to"].to_list()))
nodes_gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries(points), crs=28992)
nodes_gdf["node_id"] = nodes_gdf.index + 1

links_gdf["node_to"] = gdf["point_to"].apply(lambda x: nodes_gdf.at[nodes_gdf.distance(x).idxmin(), "node_id"])
links_gdf["node_from"] = gdf["point_from"].apply(lambda x: nodes_gdf.at[nodes_gdf.distance(x).idxmin(), "node_id"])


def get_node_type(node_id):
    if (node_id in links_gdf["node_to"].values) & (node_id in links_gdf["node_from"].values):
        node_type = "connection"
    elif node_id in links_gdf["node_to"].values:
        node_type = "ds_boundary"
    else:
        node_type = "us_boundary"
    return node_type

nodes_gdf["type"] = nodes_gdf["node_id"].apply(lambda x: get_node_type(x))

links_gdf.to_file("links.gpkg")
nodes_gdf.to_file("nodes.gpkg")


