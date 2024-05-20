from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import datetime
import glob
import os
import time

import dateutil
import pandas as pd
from sqlalchemy import Engine
import timescaledb_model as tsdb
from data_pre_process import insert_day_stocks, insert_stocks, get_day, delete_day, check_days, clean_data, split_dataframe
from companies import insert_companies



# db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")  # inside docker
db = tsdb.TimescaleStockMarketModel(
 "bourse", "ricou", "localhost", "monmdp"
)  # outside docker

engine = db.get_engine()

def initializer():
    """ensure the parent proc's database connections are not touched
    in the new connection pool"""
    engine.dispose(close=False)


def run_in_process(df: pd.DataFrame):
    with engine.connect() as conn:
        insert_stocks(df, conn)


def parallel_insertion(df_days: pd.DataFrame):
    pool = Pool(processes=8, initializer=initializer)
    df_days_split = split_dataframe(df_days, 8)
    
    try:
        pool.map(run_in_process, [df for df in df_days_split])
    except Exception as e:
        print(e)

def read_pickle(file_path: str):
    parts = file_path.split(' ', maxsplit=1)
    date = parts[1][:-4]
    timestamp = dateutil.parser.parse(date)
    return timestamp, pd.read_pickle(file_path)

def parallel_read_pickles(matching_files: list[str], max_workers=None) -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(read_pickle, f) for f in matching_files]
        raw_data = pd.concat({timestamp: data for timestamp, data in [future.result() for future in futures]}, 
                             names=['timestamp', 'symbol_index'])
    return raw_data

def companies(db: tsdb.TimescaleStockMarketModel, engine: Engine):

    file_pattern = "./data/boursorama/**/*16:0*"
    file_pattern1 = "./data/boursorama/**/*9:0*"

    # Récupérer les fichiers correspondants au pattern
    matching_files0 = glob.glob(file_pattern, recursive=True)
    matching_files1 = glob.glob(file_pattern1, recursive=True)

    matching_files = matching_files0 + matching_files1
    raw_data = parallel_read_pickles(matching_files, max_workers=os.cpu_count()) # read data
    symbol_cid_mapping = insert_companies(raw_data, db, engine=engine)
    return symbol_cid_mapping



def stocks(db: tsdb.TimescaleStockMarketModel, symbol_cid_mapping: dict[str, int], file_pattern: str, batch_size: int, engine: Engine):
    processed_files = set()  # Initialize an empty set to store processed file names

    # Récupérer les fichiers correspondants au pattern
    file_list = glob.glob(file_pattern, recursive=True)
    file_list.sort()
    df_days: pd.DataFrame = pd.DataFrame()

    start_time = time.time()

    for i in range(0, len(file_list), batch_size):
        batch_files = file_list[i:i+batch_size]
    
        start_time_preprocess = time.time()
        combined_df = parallel_read_pickles(batch_files, max_workers=os.cpu_count())
        combined_df["date"] = combined_df.index.get_level_values(0).strftime("%Y-%m-%d")
        combined_df, symbol_cid_mapping_updated = clean_data(combined_df, symbol_cid_mapping, db)

        if symbol_cid_mapping_updated != None:
            symbol_cid_mapping.update(symbol_cid_mapping_updated)

        end_time_preprocess = time.time()
        print(f"Preprocessing {len(batch_files)} files in {end_time_preprocess - start_time_preprocess:.2f} seconds")

        start_time_insertion = time.time()

        parallel_insertion(combined_df)

        end_time_insertion = time.time()
        print(f"Insertion {len(batch_files)} files in {end_time_insertion - start_time_insertion:.2f} seconds")

        df_days = pd.concat([df_days, combined_df])

        number_of_days = check_days(df_days) - 1

        days = df_days['date'].unique()[0:(number_of_days-1)]
        insert = get_day(df_days, days)
        df_days = delete_day(df_days, days)
        with engine.connect() as conn:
            insert_day_stocks(insert, conn)

        # Add processed file names to the set
        processed_files.update(batch_files)

        # Clear the data frame to free up memory
        combined_df = None
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Processed {len(processed_files)} files in {total_time:.2f} seconds for {len(symbol_cid_mapping)} companies.")


if __name__ == "__main__":
    print("Started Analyzer")
    symbole_cid: dict[str, int] = companies(db, engine=engine)
    stocks(db, symbole_cid, file_pattern="./data/boursorama/2019/amsterdam*", batch_size=500, engine=engine)
    print("Finished Analyzer")