import xarray as xr
import pandas as pd

from config import DATA_DIR

input_mozart_ds = xr.open_dataset(DATA_DIR / r"ribasim_testmodel/simplified_SAQh.nc")

da = input_mozart_ds.profile.transpose("node", "profile_col", "profile_row")
df = pd.DataFrame(
    [item.T for sublist in da.values for item in sublist.T],
    columns = ["storage", "area","discharge", "level"]
    )
df["lhm_id"] = [str(int(x)) for x in da.node.values for _ in range(4)]