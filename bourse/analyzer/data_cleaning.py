import pandas as pd
from data_utils import str_to_float

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


def clean_data(df, date, market_alias):
    # Convert 'last' and 'volume' columns to float
    df["last"] = df["last"].apply(str_to_float)
    df["volume"] = df["volume"].apply(str_to_float)

    # Drop rows with missing values
    df = df.dropna()

    # Remove duplicate rows
    df = df.drop_duplicates()

    # df for companies table
    companies_df = df.loc[:, df.columns.drop(["last", "volume"])]
    companies_df = companies_df.drop_duplicates(subset=["symbol"])
    companies_df["mid"] = MARKET_IDS[market_alias]

    # df for stocks table (cid column need to be processed by getting the company id in the db)
    stocks_df = df.loc[:, df.columns.drop(["name"])]
    stocks_df.rename(columns={"symbol": "cid", "last": "value"}, inplace=True)
    stocks_df["date"] = date

    print(companies_df)
    print(stocks_df)

    return (companies_df, stocks_df)
