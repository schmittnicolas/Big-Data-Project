from dataclasses import dataclass
import pandas as pd


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

    def __post_init__(self):
        self.clean_data()
        print("Data cleaned")
        self.df.rename_axis(["date", "symbol"], axis=0, inplace=True)
        self.df = self.df.swaplevel(0, 1).sort_index()
        self.df["cid"] = pd.factorize(self.df["symbol"])[0]
        self.df.drop(columns=['symbol'], inplace=True)
        self.df['day'] = self.df.index.get_level_values(1).strftime('%Y-%m-%d')


    def clean_data(self):
        # Convert 'last' columns to float
        self.df["last"] = (
            self.df["last"]
            .astype(str)
            .replace({" ": "", "\(c\)": "", "\(s\)": ""}, regex=True)
            .astype(float)
        )
        # Drop rows with missing values
        self.df.dropna(inplace=True)
        # Remove duplicate rows
        self.df.drop_duplicates(inplace=True)
        # Remove rows with volume = 0
        self.df = self.df[self.df["volume"] != 0]

    def get_day_stocks(self) -> pd.DataFrame:
        df_per_day = self.df.groupby(["symbol", "day"]).agg(
            min=("last", "min"),
            max=("last", "max"),
            cid=("cid", lambda x: (x.iloc[0])),
            first=("last", lambda x: (x.iloc[0])),
            last=("last", lambda x: (x.iloc[-1])),
            volume=("volume", lambda x: (x.iloc[-1])),
        )
        df_per_day.reset_index(inplace=True)
        columns_to_keep = ['day', 'min', 'max', 'first', 'last', 'volume', 'cid']
        return df_per_day[columns_to_keep]

    def get_companies(self) -> pd.DataFrame:
        df_companies = self.df.groupby("symbol").agg(name=("name", lambda x: x.iloc[0]))
        df_companies["mid"] = df_companies.index.map(self.get_market_data)
        df_companies.reset_index(inplace=True)
        columns_to_keep = ['symbol', 'name', 'mid']
        return df_companies[columns_to_keep]

    def get_stocks(self) -> pd.DataFrame:
        df_stocks = self.df.copy()
        columns_to_keep = ['date', 'last', 'volume', 'cid']
        df_stocks.reset_index(inplace=True)
        df_stocks = df_stocks[columns_to_keep]
        return df_stocks

    def get_market_data(self, symbol: str) -> str:
        if symbol[:2] == "1r":
            if self.type == "compA":
                return MARKET_IDS["compA"]
            elif self.type  == "compB":
                return MARKET_IDS["compB"]
        elif symbol[:3] == "FF1":
            return MARKET_IDS["bruxelle"]
        elif self.type  == "amsterdam":
            return MARKET_IDS["amsterdam"]
