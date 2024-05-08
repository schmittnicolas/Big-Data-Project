import time
import pandas as pd
import timescaledb_model as tsdb
import mylogging

logger = mylogging.getLogger(__name__)

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
}


def insert_companies(raw_data: pd.DataFrame, db: tsdb.TimescaleStockMarketModel):
    start_time = time.time()

    df_companies = raw_data.groupby("symbol").agg(name=("name", lambda x: x.iloc[0]))
    df_companies["mid"] = df_companies.index.map(get_market_data)
    df_companies.reset_index(inplace=True)
    db.insert_df_to_table(df=df_companies, table="companies")

    cids = [db.get_company_id(symbol) for symbol in df_companies["symbol"].unique()]
    symbol_cid_mapping = dict(zip(df_companies["symbol"].unique(), cids))

    end_time = time.time()
    # print(f"Insert Companies: {total_time} seconds")
    logger.info(f"Insert Companies: {end_time - start_time} seconds")

    return symbol_cid_mapping


def get_market_data(symbol: str) -> str:
    if symbol[:3] == "1rP":
        return MARKET_IDS["paris"]
    if symbol[:4] == "1rEP":
        return MARKET_IDS["paris"]
    if symbol[:3] == "FF1":
        return MARKET_IDS["bruxelle"]
    if symbol[:3] == "1rA":
        return MARKET_IDS["amsterdam"]
    else:
        return MARKET_IDS["xetra"]
