import datetime
import glob

import dateutil
import numpy as np
import pandas as pd
import sklearn
import timescaledb_model as tsdb
from data_pre_process import DataPreProcessor

# db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")  # inside docker
db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker


def store_file(type: str, years: str):
    """
    if db.is_file_done(name):
        return

    if website.lower() == "boursorama":
        try:
            df = pd.read_pickle("./data/boursorama/" + name)
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("./data/boursorama/" + year + "/" + name)
    """
    file_pattern = f"Big-Data-Project/bourse/data/boursorama/{years}/{type}*"
    raw_data = pd.concat({
        dateutil.parser.parse(f.split(f"{type} ")[1][:-4]): pd.read_pickle(f)
        for f in glob.glob(file_pattern)
    })

    data: DataPreProcessor = DataPreProcessor(raw_data, type)
    data_per_day = data.get_day_stocks()
    companies = data.get_companies()
    stocks = data.get_stocks()

    return data_per_day, companies, stocks



if __name__ == "__main__":
    element_1, element_2, element_3 = store_file('compA', '2019')
    element_1.to_csv('data_per_day.csv')
    element_2.to_csv('companies.csv')
    element_3.to_csv('stocks.csv')
    print("Done")