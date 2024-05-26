from typing import List
import numpy as np
import pandas as pd
from sqlalchemy import Engine
import timescaledb_model as tsdb
import re
from companies import insert_companies

def remove_non_numeric(x: str):
    return re.sub(r"[() cs]+", "", x)

def clean_data(df: pd.DataFrame, symbol_cid_mapping: dict[str, int], ) -> tuple[pd.DataFrame, dict[str, int] | None]:
    df = df[df["volume"] > 0]
    df["cid"] = df.index.get_level_values(1).map(symbol_cid_mapping)
    if df['cid'].isna().any():
        df.dropna(subset=["cid"], inplace=True)
        df['cid'] = df['cid'].astype(int)
    df["last"] = (
        df["last"]
        .astype(str)
        .apply(remove_non_numeric)
        .astype(float)
        .round(2)
    )
    
    return df 

def insert_day_stocks(df: pd.DataFrame,  connection) -> pd.DataFrame:
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
    tsdb.insert_daystocks_to_db(df=df_per_day[columns_to_keep], conn=connection)

def insert_stocks(df: pd.DataFrame,  connection) -> pd.DataFrame:
    df_stocks = df.rename(columns={"last": "value"})

    columns_to_keep = ["date", "cid", "value", "volume"]
    df_stocks.reset_index(inplace=True)
    df_stocks.drop(columns=['date'], inplace=True)
    df_stocks = df_stocks.rename(columns={"timestamp": "date"})
    df_stocks = df_stocks[columns_to_keep]
    tsdb.insert_stocks_to_db(df=df_stocks[columns_to_keep], conn=connection)



def check_days(df: pd.DataFrame):
    return len(df['date'].unique()) 

def delete_day(df: pd.DataFrame, days: list[str]):
    df = df[~df['date'].isin(days)]
    return df

def get_day(df: pd.DataFrame, day: str):
    day_data = df[df['date'] == day]
    return day_data


def split_dataframe(df: pd.DataFrame, num_splits: int) -> List[pd.DataFrame]:
    return np.array_split(df, num_splits)