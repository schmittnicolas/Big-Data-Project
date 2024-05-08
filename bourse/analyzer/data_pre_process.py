import pandas as pd
import timescaledb_model as tsdb


def convertion(str_stock):
    try:
        return float(str_stock)
    except:
        return float(str_stock.replace(" ", "").replace("(c)", "").replace("(s)", ""))


def clean_data(df: pd.DataFrame, symbol_cid_mapping: dict):
    df["cid"] = df.index.get_level_values(1).map(symbol_cid_mapping)
    # df["last"] = (
    #     df["last"]
    #     .astype(str)
    #     .replace({" ": "", "(c)": "", "(s)": ""})
    #     .astype(float)
    #     .round(2)
    # )

    df["last"] = df["last"].apply(convertion)
    df["volume"] = df["volume"].astype('int')
    df.dropna(inplace=True)
    df = df[df["volume"] > 0]

    return df


def insert_day_stocks(
    df: pd.DataFrame, db: tsdb.TimescaleStockMarketModel
) -> pd.DataFrame:
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
    columns_to_keep = [
        "date",
        "high",
        "low",
        "open",
        "close",
        "volume",
        "cid",
        "mean",
        "standard_deviation",
    ]
    db.insert_df_to_table(df=df_per_day[columns_to_keep], table="daystocks")


def insert_stocks(df: pd.DataFrame, db: tsdb.TimescaleStockMarketModel) -> pd.DataFrame:
    df_stocks = df.rename(columns={"last": "value"})

    columns_to_keep = ["date", "value", "volume", "cid"]
    df_stocks.reset_index(inplace=True)
    df_stocks.drop(columns=["date"], inplace=True)
    df_stocks = df_stocks.rename(columns={"timestamp": "date"})
    df_stocks = df_stocks[columns_to_keep]
    db.insert_df_to_table(df=df_stocks[columns_to_keep], table="stocks")


def check_days(df: pd.DataFrame):
    return len(df["date"].unique())


def delete_day(df: pd.DataFrame, days: list[str]):
    df = df[~df["date"].isin(days)]
    return df


def get_day(df: pd.DataFrame, days: list[str]):
    df = df[df["date"].isin(days)]
    return df
