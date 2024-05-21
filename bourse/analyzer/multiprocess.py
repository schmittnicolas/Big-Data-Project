
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import os
import time
import traceback

import dateutil
import pandas as pd
import psycopg2
from data_pre_process import get_day, insert_day_stocks, insert_stocks, split_dataframe
import mylogging



db_params = {
    "database": "bourse",
    "user": "ricou",
    "password": "monmdp",
    "host": "db",
}

logger = mylogging.getLogger(__name__, filename="/tmp/bourse_multi.log")

def get_connection():
    start_time = time.time()
    t = psycopg2.connect(**db_params)
    end_time = time.time()
    print(f"Connection in : {end_time - start_time:.2f} seconds")
    return t

def insert_db(df: pd.DataFrame):
    try:
        conn = get_connection()
        logger.info(f"Inserting data for {df['date'].unique()}")
        insert_stocks(df, connection=conn)
        logger.info(f"Successfully inserted data for {df['date'].unique()}")
    except Exception as e:
        logger.error(f"Error inserting data for {df['date'].unique()}: {str(e)}")
    finally:
        conn.commit()
        conn.close()


def parallel_insertion(df_days: pd.DataFrame):
    try:

        start_time = time.time()
        # Do the parallel insertion into DB

        number_of_splits = os.cpu_count() - 1


        pool_args = split_dataframe(df=df_days, num_splits=number_of_splits)

        
        logger.info("Starting parallel insertion")

        pool = Pool(processes=number_of_splits)

        pool.map(insert_db, pool_args)

        pool.close()
        pool.join()

        end_time = time.time()
        print(f"Parallel insertion in : {end_time - start_time:.2f} seconds")
        
      
    except Exception as e:
        print(traceback.format_exc())
        logger.error(f"Error during parallel insertion: {str(e)}")
        logger.error(traceback.format_exc())


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