import pandas as pd



def clean_data(df):
    # Define column names based on the companies table
    column_names = ['value', 'volume', 'company_name', 'company_symbol']

    # Create an empty DataFrame with column names
    df = pd.DataFrame(columns=column_names)

    return df