import streamlit as st
import pandas as pd
import ipaddress
import pytricia
import os
import json
import re
from typing import Optional

drop_chars = [
    ".",
    ",",
    "+",
    "'",
    '"',
    "-",
    "–",
    "_",
    ":",
    "/",
    "\\",
    ":",
    "*",
    "#",
    "|",
]
replace_chars = {
    "&amp;": "&",
    "&quot;": '"',
    "&lt;": "<",
    "&gt;": ">",
}


def string_cleaning_lvl0_str(name: str):
    name = name.lower().strip()
    for char in drop_chars:
        name = name.replace(char, " ")
    for k, v in replace_chars.items():
        name = name.replace(k, v)
    name = re.sub(r"\s+", " ", name)
    return name


class IPLookupService:
    def __init__(self, curr_date: str):
        """Initialize the lookup service with the data file."""
        self.pyt = {}
        self.pyt[4] = pytricia.PyTricia()
        self.pyt[6] = pytricia.PyTricia(128)
        self.pfx_v4 = pytricia.PyTricia()
        self.pfx_v6 = pytricia.PyTricia(128)
        self.df_pfx2org = pd.DataFrame()
        self.df_asn = pd.DataFrame()
        self.org_size_dict = {}
        self.curr_date = curr_date

        self.load_data()

    def load_data(self):
        """Load the data file."""
        path_pfx2org_v4 = os.path.join(
            os.getcwd(), "data", f"prefix_tags_{self.curr_date}_v4.parquet"
        )
        print(f"Loading data from {path_pfx2org_v4}")
        self.df_v4 = pd.read_parquet(path_pfx2org_v4)
        self.df_v4["af"] = 4

        path_pfx2org_v6 = os.path.join(
            os.getcwd(), "data", f"prefix_tags_{self.curr_date}_v6.parquet"
        )
        print(f"Loading IPv6 data from {path_pfx2org_v6}")
        self.df_v6 = pd.read_parquet(path_pfx2org_v6)
        self.df_v6["af"] = 6

        self.df_pfx2org = pd.concat([self.df_v4, self.df_v6], ignore_index=True)
        
        self.df_pfx2org["origin_asn"] = "AS" + self.df_pfx2org["origin_asn"].astype(str)
        self.df_pfx2org["asn_cluster"] = "AS" + self.df_pfx2org["asn_cluster"].astype(str)

        path_asn = os.path.join(os.getcwd(), "data", "df_master_as.parquet")
        self.df_asn = pd.read_parquet(path_asn)

        for pfx in self.df_pfx2org[self.df_pfx2org["af"] == 4].prefix.unique():
            self.pfx_v4[pfx] = True
        for pfx in self.df_pfx2org[self.df_pfx2org["af"] == 6].prefix.unique():
            self.pfx_v6[pfx] = True

        for af in [4, 6]:
            dd = self.df_pfx2org[self.df_pfx2org.af == af][
                [
                    "prefix",
                    "root_org_name",
                    "root_net_type",
                    "org_name",
                    "net_type",
                    "origin_asn",
                ]
            ]
            mask = ~dd.prefix.duplicated(keep=False)

            df_curr = dd[mask].set_index("prefix")
            for k, v in df_curr.to_dict(orient="index").items():
                self.pyt[af][k] = [v]

            df_curr = dd[~mask].set_index("prefix")
            grp = df_curr.groupby(df_curr.index)
            for pfx, df_grp in grp:
                self.pyt[af][pfx] = df_grp.to_dict(orient="records")

        with open(os.path.join(os.getcwd(), "data", "org_size_dict.json"), "r") as f:
            self.org_size_dict = json.load(f)

    def search_by_prefix(self, prefix: str) -> Optional[pd.Series]:
        """Search for an exact prefix match."""
        try:
            # Validate IP prefix format
            ipaddress.ip_network(prefix)
            if "." in prefix:
                prefix = self.pfx_v4.get_key(prefix)
            else:
                prefix = self.pfx_v6.get_key(prefix)
            df_res = self.df_pfx2org[self.df_pfx2org["prefix"] == prefix]
            if df_res.empty:
                return None
            else:
                return df_res
        except ValueError:
            return None

    def search_by_asn(self, asn: str) -> pd.DataFrame:
        """Search for entries matching the ASN."""
        try:
            asn = asn.replace("AS", "")
            int(asn)
        except ValueError:
            return pd.DataFrame()
        return self.df_pfx2org[
            self.df_pfx2org["origin_asn"] == "AS" + str(asn)
        ].reset_index(drop=True)

    def search_by_organization(self, org_name: str) -> pd.DataFrame:
        """Search for entries matching the organization name."""
        return self.df_pfx2org[
            self.df_pfx2org["root_org_name"].str.contains(
                org_name, case=False, na=False
            )
        ].reset_index(drop=True)

    def get_asn_info(self, asn: str) -> Optional[pd.Series]:
        """Get information about the ASN."""
        try:
            asn = asn.replace("AS", "")
            int(asn)
        except ValueError:
            return None
        return (
            self.df_asn[self.df_asn["origin_asn"] == int(asn)].iloc[0]
            if not self.df_asn[self.df_asn["origin_asn"] == int(asn)].empty
            else None
        )

    def get_roa_list(self, pfx, roa_list):
        def get_af(pfx):
            return 6 if ":" in pfx else 4

        af = get_af(pfx)
        pfx_len = int(pfx.split("/")[1])

        record_list = self.pyt[af][pfx]
        child_list = self.pyt[af].children(pfx)

        for child in child_list:
            child_roa_list = self.get_roa_list(child, roa_list=roa_list)
            roa_list = child_roa_list

        for record in record_list:
            roa_list.append(
                {
                    "prefix": pfx,
                    "origin_asn": record["origin_asn"],
                    "max_len": pfx_len,
                }
            )
        return roa_list


def main():
    tool_name = "ru-RPKI-ready"
    st.set_page_config(page_title=tool_name, layout="wide")

    st.title(tool_name)

    # Initialize the service
    @st.cache_resource
    def load_service():
        return IPLookupService("2025-04-01")

    try:
        service = load_service()
    except FileNotFoundError:
        st.error(
            "Data file not found. Please ensure the data files exist in the application directory."
        )
        return

    # Create tabs for different search types
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Prefix Search", "ASN Search", "Organization Search", "Generate ROAs"]
    )

    # Prefix Search Tab
    with tab1:
        st.header("Search by IP Prefix")
        prefix_input = st.text_input(
            "Enter IP prefix (e.g., 216.1.81.0/24)", key="prefix_search"
        )

        if st.button("Search Prefix", key="search_prefix_btn"):
            if prefix_input:
                try:
                    result = service.search_by_prefix(prefix_input)
                    if result is not None:
                        st.subheader("Result:")
                        col1, col2 = st.columns(2)

                        if len(result) > 1:
                            st.subheader(f"Found {len(result)} results:")
                            st.dataframe(
                                result[
                                    [
                                        "prefix",
                                        "root_org_name",
                                        "root_net_type",
                                        "origin_asn",
                                        "rpki_status",
                                        "tag_list",
                                        "ski",
                                        "org_name",
                                        "net_type",
                                    ]
                                ]
                            )
                        else:
                            result = result.iloc[0]
                            with col1:
                                st.write("**Prefix:**", result["prefix"])
                                st.write(
                                    f"**Origin ASN:** {result['origin_asn']}",
                                )
                                st.write("**RPKI Status:**", result["rpki_status"])
                                st.write("**Tags:**", result["tag_list"])
                                if result["ski"]:
                                    st.write("**RC:**", result["ski"])
                                else:
                                    st.write("**RC:**", "N/A")
                            with col2:
                                st.write("**Organization:**", result["root_org_name"])
                                st.write(
                                    "**Allocation Status:**", result["root_net_type"]
                                )
                                st.write(
                                    "**Customer Organization:**", result["org_name"]
                                )
                                st.write(
                                    "**Customer Allocation Status:**",
                                    result["net_type"],
                                )
                                st.write("**Country:**", result["country"])
                    else:
                        st.warning("No matching prefix found.")
                except ValueError:
                    st.error("Invalid IP prefix format.")
            else:
                st.warning("Please enter an IP prefix.")

    # ASN Search Tab
    with tab2:
        st.header("Search by ASN")
        asn_input = st.text_input("Enter ASN (e.g., AS15169)", key="asn_search")

        if st.button("Search ASN", key="search_asn_btn"):
            if asn_input:
                results = service.search_by_asn(asn_input)
                as_info = service.get_asn_info(asn_input)
                if not results.empty:
                    col1, col2 = st.columns(2)
                    with col1:
                        if as_info is not None:
                            st.write("**Date:**", as_info["date"])
                            st.write("**ASN:**", asn_input)
                            st.write("**Organization:**", as_info["org_name"])
                            st.write("**OrgID:**", as_info["org_id"])
                            st.write(
                                "**Number of customer organizations:**",
                                results.root_org_name.nunique(),
                            )
                            st.write(
                                "**ROA Cover (by prefix):**",
                                as_info["roa_cover_pfx_count"],
                                "%",
                            )
                            st.write(
                                "**ROA Cover (by address):**",
                                as_info["roa_cover_addr_space"],
                                "%",
                            )
                    with col2:
                        st.write("**Customer Organizations and prefix count:**")
                        st.dataframe(results.root_org_name.value_counts())
                    st.subheader(f"Found {len(results)} results:")
                    st.dataframe(results)
                else:
                    st.warning("No matching ASN found.")
            else:
                st.warning("Please enter an ASN.")

    # Organization Search Tab
    with tab3:
        st.header("Search by Organization")
        org_input = st.text_input("Enter organization name", key="org_search")

        if st.button("Search Organization", key="search_org_btn"):
            if org_input:
                results = service.search_by_organization(org_input)
                if not results.empty:
                    proc_org_input = string_cleaning_lvl0_str(org_input)
                    if proc_org_input in service.org_size_dict:
                        st.subheader("Result:")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**Organization:**", org_input)
                            st.write("**Tags:**", service.org_size_dict[proc_org_input])
                        with col2:
                            pass
                    st.subheader(f"Found {len(results)} results:")
                    st.dataframe(results)
                else:
                    st.warning("No matching organizations found.")
            else:
                st.warning("Please enter an organization name.")

    # ROA-List Tab
    with tab4:
        st.header("Generate ROAs")
        prefix_input = st.text_input(
            "Enter IP prefix (e.g., 8.48.0.0/12)", key="prefix_input"
        )

        if st.button("Generate ROAs", key="generate_roas_btn"):
            if prefix_input:
                try:
                    roa_list = []
                    roa_list = service.get_roa_list(prefix_input, roa_list)
                    if roa_list:
                        st.subheader("List of ROAs to Create:")
                        st.dataframe(roa_list)
                    else:
                        st.warning("No ROAs generated for the given prefix.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")
            else:
                st.warning("Please enter a valid IP prefix.")

    # Show sample data
    with st.expander("View Sample Data", expanded=False):
        st.subheader("Sample Records")
        df = service.df_pfx2org.sample(10, random_state=42).reset_index(drop=True)
        st.dataframe(df, use_container_width=True)

        st.subheader("Dataset Schema")
        schema_info = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            null_count = df[col].isnull().sum()
            schema_info.append(
                {
                    "Column": col,
                    "Data Type": dtype,
                    "Null Values": null_count,
                    "Sample Value": str(df[col].iloc[0]) if len(df) > 0 else "N/A",
                }
            )

        schema_df = pd.DataFrame(schema_info)
        st.dataframe(schema_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
