import pandas as pd

def clean_data(df):
    # Define column names based on the companies table
    column_names = ['id', 'name', 'mid', 'symbol', 'symbol_nf', 'isin', 'reuters', 'boursorama', 'pea', 'sector']

    # Create an empty DataFrame with column names
    df_empty = pd.DataFrame(columns=column_names)

    return df