import time
import pandas as pd
import psycopg2
import psycopg2.pool
from sqlalchemy import Engine
import timescaledb_model as tsdb

MARKET_IDS = {
    "euronx": 1,
    "lse": 2,
    "milano": 3,
    "dbx": 4,
    "mercados": 5,
    "amsterdam": 6,
    "xetra": 9,
    "bruxelle": 10,
    "paris": 11,
    "nasdaq": 12,
}


def insert_companies(raw_data: pd.DataFrame,db: tsdb.TimescaleStockMarketModel) -> dict[str, int]:
    
    df_companies = raw_data.groupby("symbol").agg(name=("name", lambda x: x.iloc[0]))
    df_companies["mid"] = df_companies.index.map(get_market_data)
    df_companies.reset_index(inplace=True)
    df_companies.index.name = 'id'
    conn = db.get_connection()
    tsdb.insert_companies_to_db(df=df_companies, conn=conn)
    cids = [db.get_company_id(value) for value in df_companies["symbol"].unique()]
    symbol_cid_mapping: dict[str, int] = dict(zip(df_companies["symbol"].unique(),  cids))
    return symbol_cid_mapping
    
def get_market_data(symbol: str) -> str:
    if symbol[:3] == "1rP" :
       return MARKET_IDS["paris"]
    if symbol[:4] == "1rEP":
        return MARKET_IDS["paris"]
    if symbol[:3] == "FF1":
        return MARKET_IDS["bruxelle"]
    if symbol[:3] == "1rA":
        return MARKET_IDS["amsterdam"]
    else:
        return MARKET_IDS["nasdaq"]