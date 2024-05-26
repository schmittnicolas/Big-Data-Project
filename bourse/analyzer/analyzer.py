import glob
from multiprocessing import Pool
import os
import time

import pandas as pd
from multiprocess import parallel_insertion, parallel_read_pickles, get_connection
import timescaledb_model as tsdb
from data_pre_process import insert_day_stocks, insert_stocks, delete_day, check_days, clean_data
from companies import insert_companies



# db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "db", "monmdp")  # inside docker
#db = tsdb.TimescaleStockMarketModel("bourse", "ricou", "localhost", "monmdp")  # inside docker

db_params = {
    "database": "bourse",
    "user": "ricou",
    "password": "monmdp",
    "host": "db",
}

db = tsdb.TimescaleStockMarketModel(**db_params)


def companies(db: tsdb.TimescaleStockMarketModel):


    file_pattern = "./data/boursorama/**/*16:0*"
    file_pattern1 = "./data/boursorama/**/*9:0*"

    # Récupérer les fichiers correspondants au pattern
    matching_files0 = glob.glob(file_pattern, recursive=True)
    matching_files1 = glob.glob(file_pattern1, recursive=True)

    matching_files = matching_files0 + matching_files1
    raw_data = parallel_read_pickles(matching_files, max_workers=os.cpu_count()) # read data
    symbol_cid_mapping = insert_companies(raw_data, db=db) # insert data
    return symbol_cid_mapping



def stocks(symbol_cid_mapping: dict[str, int], file_pattern: str,  batch_size):
    
    
    processed_files = set()  # Initialize an empty set to store processed file names
   
    # Récupérer les fichiers correspondants au pattern
    file_list = glob.glob(file_pattern, recursive=True)
    file_list.sort()
    df_days: pd.DataFrame = pd.DataFrame()

    start_time = time.time()

    for i in range(0, len(file_list), batch_size):
        batch_files = file_list[i:i+batch_size]
    
        start_time2 = time.time()

        import pdb; pdb.set_trace()

        combined_df = parallel_read_pickles(batch_files, max_workers=os.cpu_count())
        combined_df["date"] = combined_df.index.get_level_values(0).strftime("%Y-%m-%d")
        combined_df = clean_data(combined_df, symbol_cid_mapping)



        end_time2 = time.time()


        print(f"Pre-Processed done in {end_time2 - start_time2:.2f} seconds for {len(batch_files)} files.")

        parallel_insertion(combined_df)


        df_days = pd.concat([df_days, combined_df])


        number_of_days = check_days(df_days) - 1


        days = df_days['date'].unique()[0:number_of_days]
        print(f"Inserting {days} days.")
        try:
            connection_day = get_connection()
            insert_day_stocks(df_days, connection=connection_day)
        except Exception as e:
            print(e)
        finally:
            connection_day.commit()
            connection_day.close()
        df_days = delete_day(df_days, days)
        
        
        processed_files.update(batch_files)
    
    print(f"final insertion is {df_days} ")
    try:
        conn = get_connection()
        insert_stocks(df_days, connection=conn)
        insert_day_stocks(df_days, connection=conn)
    except Exception as e:
            print(e)
    finally:
        conn.commit()
        conn.close()
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Processed {len(processed_files)} files in {total_time:.2f} seconds for {len(symbol_cid_mapping)} companies.")

if __name__ == "__main__":
    print("Started Analyzer")
    cid_mapping = companies(db)
    db.__connection.close()
    stocks(symbol_cid_mapping=cid_mapping, file_pattern="./data/boursorama/*/*", batch_size=150)
   
    

