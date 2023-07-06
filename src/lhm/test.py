from dataclasses import dataclass, field
import geopandas as gpd
import networkx as nx

NODE_COLUMNS = ["index", "geometry"]
LINK_COLUMNS = ["index", "node_from", "node_to", "flow_type", "geometry"]

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

    def set_nodes(self, nodes=None):
        if nodes is not None:
            self._nodes = nodes[NODE_COLUMNS].copy()
        else:
            self._nodes = gpd.GeoDataFrame(columns=NODE_COLUMNS, crs=self.crs)
        self._graph = None
    
    def set_links(self, links=None):
        if links is not None:
            self._links = links[LINK_COLUMNS].copy()
        else:
            self._links = self._links[LINK_COLUMNS].copy()
        self._graph = None

    @property
    def nodes(self):
        return self._nodes

    @property
    def links(self):
        return self._links
