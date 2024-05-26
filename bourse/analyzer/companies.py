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
    "xetra": 9,
    "bruxelle": 10,
    "paris": 11,
}


def insert_companies(raw_data: pd.DataFrame, db: tsdb.TimescaleStockMarketModel) -> dict[str, int]:
    start_time = time.time()  # get start time before insert
    df_companies = raw_data.groupby("symbol").agg(name=("name", lambda x: x.iloc[0]))
    df_companies["mid"] = df_companies.index.map(get_market_data)
    df_companies.reset_index(inplace=True)
    db.insert_df_to_table(df=df_companies, table="companies")
    cids = [db.get_company_id(value) for value in df_companies["symbol"].unique()]
    symbol_cid_mapping: dict[str, int] = dict(zip(df_companies["symbol"].unique(),  cids))
    end_time = time.time()  # get end time after insert
    total_time = end_time - start_time  # calculate the time
    print(f"Insert Companies: {total_time} seconds")  # print time
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