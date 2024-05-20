import numpy as np
import pandas as pd
from sqlalchemy import Engine
import timescaledb_model as tsdb
import re
from companies import insert_companies

def remove_non_numeric(x: str):
    return re.sub(r"[() cs]+", "", x)

def clean_data(df: pd.DataFrame, symbol_cid_mapping: dict[str, int], 
               db: tsdb.TimescaleStockMarketModel) -> tuple[pd.DataFrame, dict[str, int] | None]:
    df = df[df["volume"] > 0]
    misising_companies_cid = None
    df["cid"] = df.index.get_level_values(1).map(symbol_cid_mapping)
    missing_value = df[df["cid"].isna()]
    if len(missing_value) > 0:
        missing_companies = missing_value.groupby("symbol").agg(name=(("name", lambda x: x.iloc[0])))
        misising_companies_cid = insert_companies(missing_companies, db)
        symbol_cid_mapping.update(misising_companies_cid)
        df["cid"] = df.index.get_level_values(1).map(symbol_cid_mapping).astype(int)
        print(f"Missing companies cid {misising_companies_cid}")
    df["last"] = (
        df["last"]
        .astype(str)
        .apply(remove_non_numeric)
        .astype(float)
        .round(2)
    )
    
    return df, misising_companies_cid 

def insert_day_stocks(df: pd.DataFrame, conn) -> pd.DataFrame:
    df_per_day = df.groupby(["symbol", "date"]).agg(
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
    columns_to_keep = ["date", "high", "low", "open", "close", "volume", "cid", "mean", "standard_deviation"]
    tsdb.insert_df_to_table(df=df_per_day[columns_to_keep], table="daystocks", engine=conn)

def insert_stocks(df: pd.DataFrame, conn) -> pd.DataFrame:
    df_stocks = df.rename(columns={"last": "value"})

    columns_to_keep = ["date", "value", "volume", "cid"]
    df_stocks.reset_index(inplace=True)
    df_stocks.drop(columns=['date'], inplace=True)
    df_stocks = df_stocks.rename(columns={"timestamp": "date"})
    df_stocks = df_stocks[columns_to_keep]
    tsdb.insert_df_to_table(df=df_stocks[columns_to_keep], table="stocks", engine=conn)



def check_days(df: pd.DataFrame):
    return len(df['date'].unique()) 

def delete_day(df: pd.DataFrame, days: list[str]):
    df = df[~df['date'].isin(days)]
    return df

def get_day(df: pd.DataFrame, days: list[str]):
    df = df[df['date'].isin(days)]
    return df

def split_dataframe(df: pd.DataFrame, num_splits: int):
    return np.array_split(df, num_splits)