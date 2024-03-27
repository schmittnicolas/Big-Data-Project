import datetime

import data_utils as du
import dateutil
import numpy as np
import pandas as pd
import sklearn
import timescaledb_model as tsdb
from data_cleaning import clean_data

db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")  # inside docker
# db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker


def store_file(name, website):
    if db.is_file_done(name):
        return

    if website.lower() == "boursorama":
        try:
            df = pd.read_pickle("./data/boursorama/" + name)
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("./data/boursorama/" + year + "/" + name)

        (date, market) = du.get_date_and_market_name(name)
        (companies_df, stocks_df) = clean_data(df, date, market)
        db.add_dfs_to_sql(companies_df, stocks_df)


if __name__ == "__main__":
    store_file("compA 2020-01-01 09:02:02.532411.bz2", "boursorama")
    print("Done")
