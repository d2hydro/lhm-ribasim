import geopandas as gpd
from pathlib import Path
from read import read_dw_keys

lkm25_links = r"d:\repositories\lhm-ribasim\data\lkm25\Schematisatie\KRWVerkenner\shapes\LKM25_Links.shp"

lkm25_links_gdf = gpd.read_file(lkm25_links)

dw_keys_df = read_dw_keys(r"d:\repositories\lhm-ribasim\data\lhm4.3\dm\txtfiles_git\dwkeys.txt")