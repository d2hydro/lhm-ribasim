from ribasim import Model
from config import MODEL_DIR

model_name = "ribasim_model_nederland"
model = Model.from_toml(
    MODEL_DIR.joinpath(model_name,f"{model_name}.toml")
    )

#%% correct tables
basin_profiles_df = model.basin.profile.copy()

for node_id, df in basin_profiles_df.groupby("node_id"):
    if (df["area"] == 0).all():
        print(node_id)
        basin_profiles_df.loc[basin_profiles_df["node_id"] == node_id, ["area"]] = 100


#%% write model
model.basin.profile = basin_profiles_df

#%% write model
model.write(MODEL_DIR / model_name)