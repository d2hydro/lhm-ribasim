import networkx as nx
from dataclasses import dataclass, field
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString
import folium

NODE_COLUMNS = ["index", "geometry"]
LINK_COLUMNS = ["index", "node_from", "node_to", "flow_type", "link_type", "geometry"]

def merge_lsm_lkm(lkm25_links_gdf: gpd.GeoDataFrame, network_l_gdf: gpd.GeoDataFrame, network_n_gdf: gpd.GeoDataFrame):
    
    lkm25_lsm_links_mask = lkm25_links_gdf["node_to"].str.startswith("LKM_LSM")

    # snap LKM-LSM links to network_node_gdf nodes
    for row in lkm25_links_gdf.loc[lkm25_lsm_links_mask].itertuples():
        end_point = row.geometry.boundary.geoms[-1]
        network_l_select_gdf = network_l_gdf.iloc[network_l_gdf.sindex.intersection(end_point.buffer(100).bounds)]
        network_l_idx = network_l_select_gdf.geometry.distance(end_point).idxmin()
        end_coords = network_l_gdf.at[network_l_idx, "geometry"].coords[0]
        lkm25_links_gdf.at[row.Index, "node_to"] = network_l_gdf.at[network_l_idx, "node_from"]
        lkm25_links_gdf.at[row.Index, "geometry"] = LineString((row.geometry.coords[0], end_coords))

    # generate lkm25_nodes_gdf from lkm25_links
    lkm25_nodes_from_gdf = lkm25_links_gdf.copy()
    lkm25_nodes_from_gdf["geometry"] = lkm25_nodes_from_gdf.geometry.apply(lambda x:Point(x.coords[0]))
    lkm25_nodes_to_gdf = lkm25_links_gdf.loc[~lkm25_links_gdf["node_to"].isin(lkm25_links_gdf["node_to"])]
    lkm25_nodes_to_gdf["geometry"] = lkm25_nodes_to_gdf.geometry.apply(lambda x:Point(x.coords[0]))

    lkm25_nodes_gdf = pd.concat(
        [lkm25_nodes_from_gdf.set_index("node_from"),
        lkm25_nodes_to_gdf.set_index("node_to")]
         )

    lkm25_nodes_gdf = lkm25_nodes_gdf.loc[~lkm25_nodes_gdf.index.duplicated(keep="first")]
    lkm25_nodes_gdf = lkm25_nodes_gdf.loc[~lkm25_nodes_gdf.index.isin(network_n_gdf.index)]

    # merge LKM with LSM
    network_nodes_gdf = pd.concat([lkm25_nodes_gdf,network_n_gdf])
    network_links_gdf = pd.concat([lkm25_links_gdf, network_l_gdf])

    return network_links_gdf, network_nodes_gdf

def create_graph(network_links_gdf, network_nodes_gdf):
    graph = nx.DiGraph()
    
    # Iterate over each row in the network nodes
    for row in network_nodes_gdf.itertuples():
        graph.add_node(row.index)
    
    # Iterate over each row in network_l
    for row in network_links_gdf.itertuples():
        graph.add_edge(row.node_from, row.node_to, index=row.index, length=row.geometry.length)
    
    return graph

@dataclass
class Network():
    crs: int = 28992
    _nodes: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    _links: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    _graph: nx.DiGraph = None
    
    def __init__(self, nodes: gpd.GeoDataFrame = None, links: gpd.GeoDataFrame = None):
        if nodes is not None:
            self.set_nodes(nodes)
        if links is not None:
            self.set_links(links)

    def _set_crs(self, gdf:gpd.GeoDataFrame):
        if gdf.crs is None:
            gdf.crs = self.crs
        else:
            gdf.to_crs(self.crs, inplace=True)

    def set_nodes(self, nodes=None):
        if isinstance(nodes, gpd.GeoDataFrame):
            nodes = nodes.reset_index()[NODE_COLUMNS].copy()
            self._set_crs(nodes)
            self._nodes = nodes
        else:
            self._nodes = gpd.GeoDataFrame(columns=NODE_COLUMNS, crs=self.crs)
        self._graph = None
    
    def set_links(self, links=None):
        if isinstance(links, gpd.GeoDataFrame):
            links = links.reset_index()[LINK_COLUMNS].copy()
            self._set_crs(links)
            self._links = links
        else:
            self._links = gpd.GeoDataFrame(columns=LINK_COLUMNS, crs=self.crs)
        self._graph = None

    @property
    def nodes(self):
        return self._nodes

    @property
    def links(self):
        return self._links


    def to_gpkg(self, filename: str, **kwargs):
        """Write nodes and links to a GPKG."""
        self.nodes.to_file(filename, layer="nodes", **kwargs)
        self.links.to_file(filename, layer="links", **kwargs)

    @classmethod
    def from_gpkg(cls, filename, **kwargs):
        """Init Network from a GPKG with links and nodes."""
        nodes = gpd.read_file(filename, layer="nodes", **kwargs)
        links = gpd.read_file(filename, layer="links", **kwargs)
        links = links[links["node_from"].isin(nodes["index"]) & links["node_to"].isin(nodes["index"])]
        nodes = nodes[nodes["index"].isin(links["node_to"]) | nodes["index"].isin(links["node_from"])]
        return cls(nodes=nodes, links=links)

    @property
    def graph(self):
        """Convert nodes and links to networkx Graph."""
        if self._graph is None:
            self._graph = create_graph(self.links, self.nodes)
        return self._graph
    
    def explore(self):
        """Explore network in folium."""
        m = self.links.explore(
            column="link_type",
            cmap=["red",
                  "blue",
                  "orange",
                  "grey"]
            )

        folium.LayerControl().add_to(m)

        return m
