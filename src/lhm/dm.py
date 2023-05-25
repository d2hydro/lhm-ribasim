
def get_dm_nodes(dw_keys_df, district, mz_type ="d"):
    df = dw_keys_df[dw_keys_df.oid == district]
    return list(df[df.kty == mz_type].nid.unique())
