from dataclasses import dataclass
import time
import pandas as pd
import timescaledb_model as tsdb


MARKET_IDS = {
    "euronx": 1,
    "lse": 2,
    "milano": 3,
    "dbx": 4,
    "mercados": 5,
    "amsterdam": 6,
    "compA": 7,
    "compB": 8,
    "xetra": 9,
    "bruxelle": 10,
}


@dataclass
class DataPreProcessor:
    df: pd.DataFrame
    type: str
    db: tsdb.TimescaleStockMarketModel
    unique_symbol: list[str] = None

    def __post_init__(self):
        start_time = time.time()  # get start time before insert

        self.clean_data()
        print("Data cleaned")
        self.df.rename_axis(["date_houre", "symbol"], axis=0, inplace=True)
        self.df = self.df.swaplevel(0, 1).sort_index()
        self.df.drop(columns=["symbol"], inplace=True)
        self.df["date"] = self.df.index.get_level_values(1).strftime("%Y-%m-%d")
        self.unique_symbol = self.df.index.unique(level=0)

        end_time = time.time()  # get end time after insert
        total_time = end_time - start_time  # calculate the time
        print(f"Clean Data: {total_time} seconds")  # print time

    def clean_data(self):
        # Convert 'last' columns to float
        self.df["last"] = (
            self.df["last"]
            .astype(str)
            .replace({" ": "", "\(c\)": "", "\(s\)": ""}, regex=True)
            .astype(float)
            .round(2)
        )
        # Drop rows with missing values
        self.df.dropna(inplace=True)
        # Remove duplicate rows
        self.df.drop_duplicates(inplace=True)
        # Remove rows with volume = 0
        self.df = self.df[self.df["volume"] != 0]

    def insert_day_stocks(self) -> pd.DataFrame:
        cids = [self.db.get_company_id(value) for value in self.unique_symbol]
        symbol_cid_mapping = dict(zip(self.unique_symbol, cids))
        self.df["cid"] = self.df.index.get_level_values(0).map(symbol_cid_mapping)

        df_per_day = self.df.groupby(["symbol", "date"]).agg(
            low=("last", "min"),
            high=("last", "max"),
            cid=("cid", lambda x: (x.iloc[0])),
            open=("last", lambda x: (x.iloc[0])),
            close=("last", lambda x: (x.iloc[-1])),
            volume=("volume", lambda x: (x.iloc[-1])),
            mean=("last", lambda x: (x.mean())),
            standard_deviation=("last", lambda x: x.std()),
        )

        df_per_day.reset_index(inplace=True)
        columns_to_keep = ["date", "high", "low", "open", "close", "volume", "cid"]
        self.db.insert_df_to_table(df=df_per_day[columns_to_keep], table="daystocks")

    def insert_companies(self) -> pd.DataFrame:
        df_companies = self.df.groupby("symbol").agg(name=("name", lambda x: x.iloc[0]))
        df_companies["mid"] = df_companies.index.map(self.get_market_data)
        df_companies.reset_index(inplace=True)
        columns_to_keep = ["symbol", "name", "mid"]
        self.db.insert_df_to_table(df=df_companies[columns_to_keep], table="companies")

        # create dic for maping symbole and Id

        cids = [self.db.get_company_id(value) for value in self.unique_symbol]
        symbol_cid_mapping = dict(zip(self.unique_symbol, cids))
        self.df["cid"] = self.df.index.get_level_values(0).map(symbol_cid_mapping)

    def insert_stocks(self) -> pd.DataFrame:
        df_stocks = self.df.copy()
        df_stocks["value"] = df_stocks["last"]
        columns_to_keep = ["date_houre", "value", "volume", "cid"]
        df_stocks.reset_index(inplace=True)
        df_stocks = df_stocks[columns_to_keep]
        df_stocks.rename(columns={"date_houre": "date"}, inplace=True)
        self.db.insert_df_to_table(df=df_stocks, table="stocks")

    def get_market_data(self, symbol: str) -> str:
        if symbol[:2] == "1r":
            if self.type == "compA":
                return MARKET_IDS["compA"]
            elif self.type == "compB":
                return MARKET_IDS["compB"]
        elif symbol[:3] == "FF1":
            return MARKET_IDS["bruxelle"]
        elif self.type == "amsterdam":
            return MARKET_IDS["amsterdam"]
