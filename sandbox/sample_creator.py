import pandas as pd
import pytricia
import os

class loader:
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
        df_v4_sampled = self.df_v4.sample(5000, random_state=42)
        df_v4_sampled.to_parquet(f"data/prefix_tags_sample_{self.curr_date}_v4.parquet")

        path_pfx2org_v6 = os.path.join(
            os.getcwd(), "data", f"prefix_tags_{self.curr_date}_v6.parquet"
        )
        print(f"Loading IPv6 data from {path_pfx2org_v6}")
        self.df_v6 = pd.read_parquet(path_pfx2org_v6)
        self.df_v6["af"] = 6
        df_v6_sampled = self.df_v6.sample(5000, random_state=42)
        df_v6_sampled.to_parquet(f"data/prefix_tags_sample_{self.curr_date}_v6.parquet")

def main():
    loaded = loader("2025-04-01")

if __name__ == "__main__":
    main()