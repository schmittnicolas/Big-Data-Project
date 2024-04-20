import datetime
import glob

import dateutil
import numpy as np
import pandas as pd
import sklearn
import timescaledb_model as tsdb
from data_pre_process import DataPreProcessor

db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")  # inside docker
# db = tsdb.TimescaleStockMarketModel(
#   "bourse", "ricou", "localhost", "monmdp"
# )  # outside docker


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
    file_pattern = f"./data/boursorama/{years}/{type}*"  # outside docker
    # file_pattern = f"./data/boursorama/{years}/{type}*"
    raw_data = pd.concat(
        {
            dateutil.parser.parse(f.split(f"{type} ")[1][:-4]): pd.read_pickle(f)
            for f in glob.glob(file_pattern)
        }
    )

    data: DataPreProcessor = DataPreProcessor(raw_data, type, db)

    data.insert_companies()
    data.insert_day_stocks()
    data.insert_stocks()


if __name__ == "__main__":
    print("Started Analyzer")
    store_file("compA", "2019")
    print("done")
