import geopandas as gpd
import numpy as np
import scipy.ndimage
import xarray as xr

from config import DATA_DIR

def column_names_to_string(ds):
    ds["profile_col"] = [chr(int(v)) for v in ds["profile_col"].values]

# Read the existing (sanitized) input.
# Read the shapefiles of districts and LSW's.
# Some LSW are not part of the network. This is given by the sanitized input.

bach_ds = xr.open_dataset(DATA_DIR / r"ribasim_testmodel/bach-cases/data/1-external/input-mozart.nc")
#column_names_to_string(bach_ds)

#gdf_lsw = gpd.read_file(DATA_DIR / r"lhm4.3/coupling/lsws.shp")
#lsw_relevant = bach_ds["node"].values.astype(int)
#gdf_lsw = gdf_lsw[gdf_lsw["LSWFINAL"].isin(lsw_relevant)]
#gdf_lsw["lsw_area"] = gdf_lsw.area

# Specify the output file path relevant LSWs
#output_file_path = DATA_DIR / "lsw_relevant.gpkg"

#gdf_lsw.to_file(output_file_path)

#print("Relevant LSWs saved to:", output_file_path)

# %%
# Read the nodes file and model file
nodes_gdf = gpd.read_file("nodes.gpkg")
model_gdf = gpd.read_file(DATA_DIR / r"ribasim_testmodel/model.gpkg", layer='Node', index_col='id')

filtered_model_gdf = model_gdf[model_gdf['type'] == 'Basin']

# Perform the spatial join
joined_gdf = gpd.sjoin_nearest(filtered_model_gdf, nodes_gdf, how='left')

# Reset the index of the joined GeoDataFrame to preserve the original 'fid' values
joined_gdf.reset_index(drop=True, inplace=True)

# Set the value of 'lswfinal' to None for non-matching records
joined_gdf.loc[model_gdf['type'] != 'Basin', 'lswfinal'] = None

# Add a new column 'fid' to the joined GeoDataFrame using the index
joined_gdf['fid'] = joined_gdf.index

# Rename one of the 'TYPE' columns to avoid duplicate column names
joined_gdf.rename(columns={'TYPE': 'TYPE_2'}, inplace=True)

# Specify the output file path for the modified joined GeoDataFrame
output_file_path = "modified_joined_gdf.gpkg"

# Save the modified joined GeoDataFrame to a GeoPackage
joined_gdf.to_file(output_file_path, driver='GPKG')