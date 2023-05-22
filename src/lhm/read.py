import pandas as pd
from pathlib import Path
import re


def read_dw_keys(key_file:str) -> pd.DataFrame:
    key_file = Path(key_file)
    pattern = r'KEY oid\s+(\d+)\s+kty\s+"(\w+)"\s+rid\s+(\d+)\s+nid\s+(\d+)\s+ds\s+"(.*?)"\s+cp\s+"(.*?)"'
    matches = re.findall(pattern, key_file.read_text(), re.DOTALL)
    return pd.DataFrame(matches, columns=['oid', 'kty', 'rid', 'nid', 'ds', 'cp'])

def read_lsw_routing(dik_file:str) -> pd.DataFrame:
    dik_file = Path(dik_file)
    return pd.read_csv(
        dik_file, 
        sep= " ",
        names=["lsw_from", "lsw_to", "direction", "fraction"]
        )