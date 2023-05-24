from pathlib import Path
from shapely.geometry import Point
import pandas as pd
import geopandas as gpd

lsm3_locations = r"d:\repositories\lhm-ribasim\data\koppeling_lsm3_lhm3.4\LSM3_locations.csv"

lsm_lhm = r"d:\repositories\lhm-ribasim\data\koppeling_lsm3_lhm3.4\LSM3_DMKnoopDistrict_childs.csv"

lsm_lhm_df = pd.read_csv(lsm_lhm, sep=";").set_index("LSM3_ID")
lsm3_locations_df = pd.read_csv(lsm3_locations, sep=";").set_index("FEWS_IDs")

lsm3_locations_df = lsm3_locations_df.loc[lsm3_locations_df.index.isin(lsm_lhm_df.index)]

# %% make spatial
lsm3_locations_df["geometry"] = lsm3_locations_df.apply(
    (lambda x: Point(x.Latitude, x.Longitude)),
    axis=1
    )
lsm3_locations_gdf = gpd.GeoDataFrame(lsm3_locations_df, crs="epsg:28992")


lsm_lhm_gdf = lsm3_locations_gdf.join(lsm_lhm_df)

lsm_lhm_gdf.to_file("lsm_lhm.gpkg")