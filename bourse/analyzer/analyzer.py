from concurrent.futures import ThreadPoolExecutor
import datetime
import glob
import os
import time

import dateutil
import pandas as pd
import timescaledb_model as tsdb
from data_pre_process import (
    insert_day_stocks,
    insert_stocks,
    get_day,
    delete_day,
    check_days,
    clean_data,
)
from companies import insert_companies

import mylogging


db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")  # inside docker
# db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "localhost", "monmdp")  # outside docker

logger = mylogging.getLogger(__name__, level=mylogging.DEBUG)


def read_pickle(file_path: str):
    parts = file_path.split(" ", maxsplit=1)
    date = parts[1][:-4]

    timestamp = dateutil.parser.parse(date)

    return timestamp, pd.read_pickle(file_path)


def parallel_read_pickles(matching_files: list[str], max_workers=None) -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(read_pickle, f) for f in matching_files]
        raw_data = pd.concat(
            {
                timestamp: data
                for timestamp, data in [future.result() for future in futures]
            },
            names=["timestamp", "symbol_index"],
        )

    return raw_data


def companies(db: tsdb.TimescaleStockMarketModel):
    file_pattern = "./data/boursorama/*/*16:0*"
    file_pattern1 = "./data/boursorama/*/*9:0*"

    # Récupérer les fichiers correspondants au pattern
    matching_files0 = glob.glob(file_pattern, recursive=True)
    matching_files1 = glob.glob(file_pattern1, recursive=True)
    matching_files = matching_files0 + matching_files1

    raw_data = parallel_read_pickles(matching_files, max_workers=os.cpu_count())
    symbol_cid_mapping = insert_companies(raw_data, db)
    logger.debug(f"symbol_cid_mapping: {len(symbol_cid_mapping)}")
    # # Save the symbol_cid_mapping to a csv file
    # symbol_cid_mapping_df = pd.DataFrame(symbol_cid_mapping.items(), columns=["symbol", "cid"])
    # symbol_cid_mapping_df.to_csv("symbol_cid_mapping.csv", index=False)

    return symbol_cid_mapping


def stocks(db: tsdb.TimescaleStockMarketModel, symbol_cid_mapping: dict, batch_size=50):
    processed_files = set()  # Initialize an empty set to store processed file names
    file_pattern = "./data/boursorama/*/*"

    file_list = glob.glob(file_pattern, recursive=True)
    file_list.sort()
    df_days = pd.DataFrame()

    start_time = time.time()

    for i in range(0, len(file_list), batch_size):
        batch_start_time = time.time()

        batch_files = file_list[i : i + batch_size]

        combined_df = parallel_read_pickles(batch_files, max_workers=os.cpu_count())
        combined_df["date"] = combined_df.index.get_level_values(0).strftime("%Y-%m-%d")
        combined_df = clean_data(combined_df, symbol_cid_mapping)

        insert_stocks(combined_df, db)

        df_days = pd.concat([df_days, combined_df])

        number_of_days = check_days(df_days)

        if number_of_days != 1:
            days = df_days["date"].unique()[0 : (number_of_days - 1)]
            insert = get_day(df_days, days)
            df_days = delete_day(df_days, days)
            insert_day_stocks(insert, db)
            print(f"Inserting {days} days")

        processed_files.update(batch_files)

        # Clear the data frame to free up memory
        combined_df = None

        batch_total_time = time.time() - batch_start_time

        logger.info(f"Processed {batch_size} files from {file_list[i]} to {file_list[i + batch_size - 1]} in {batch_total_time:.2f}.")

    end_time = time.time()
    total_time = end_time - start_time

    # print(f"Processed {len(processed_files)} files in {total_time:.2f} seconds.")
    logger.info(f"Processed {len(processed_files)} files in {total_time:.2f} seconds.")


if __name__ == "__main__":
    print("Started Analyzer")
    logger.info("Started Analyzer")
    t = companies(db)
    #stocks(db, t)
