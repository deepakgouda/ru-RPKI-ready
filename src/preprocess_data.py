import os
import pandas as pd

data_path = os.getcwd()

df4 = pd.read_parquet(
    os.path.join(data_path, "data", "df_master_as_v4.parquet")
)
df6 = pd.read_parquet(
    os.path.join(data_path, "data", "df_master_as_v6.parquet")
)
df_asn = pd.concat([df4, df6])
latest_date = df_asn.date.max()
df_asn = df_asn[df_asn.date == latest_date]
df_asn_orgname = pd.read_csv(
    os.path.join(
        data_path,
        "AS2Org-CAIDA",
        f"asn_to_orgname_{latest_date.strftime('%Y-%m')}.csv",
    )
).rename(columns={"asn": "origin_asn"})
df_asn = pd.merge(df_asn, df_asn_orgname, on="origin_asn", how="left")

df_asn.roa_cover_pfx_count = (df_asn.roa_cover_pfx_count * 100).round(2)
df_asn.roa_cover_addr_space = (df_asn.roa_cover_addr_space * 100).round(2)


df_asn.to_parquet(os.path.join(data_path, "data", "df_master_as.parquet"))
