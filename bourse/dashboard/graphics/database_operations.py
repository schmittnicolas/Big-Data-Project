import pandas as pd


def get_company_data(table_name, company_id, start_date, end_date, engine):
    """
    Fetch company data from a specified table within a date range.

    Parameters:
    table_name (str): The name of the table to fetch data from.
    company_id (str): The ID of the company to fetch data for.
    start_date (str): The start of the date range.
    end_date (str): The end of the date range.
    engine (sqlalchemy.engine.Engine): The database engine to use for the query.

    Returns:
    pandas.DataFrame: A DataFrame containing the fetched data.
    """

    query = f"""
        SELECT t.*, c.name AS name, m.name AS market 
        FROM {table_name} t 
        INNER JOIN companies c ON c.id=t.cid 
        INNER JOIN markets m ON m.id=c.mid 
        WHERE c.id = %(company_id)s 
        AND t.date BETWEEN %(start_date)s AND %(end_date)s 
        ORDER BY t.date;
    """
    df = pd.read_sql_query(
        query,
        engine,
        params={
            "company_id": company_id,
            "start_date": start_date,
            "end_date": end_date,
        },
    )
    return df


def get_companies_data(table_name, company_ids, start_date, end_date, engine):
    """
    Fetch data for multiple companies from a specified table within a date range.

    Parameters:
    table_name (str): The name of the table to fetch data from.
    company_ids (List[str]): The IDs of the companies to fetch data for.
    start_date (str): The start of the date range.
    end_date (str): The end of the date range.
    engine (sqlalchemy.engine.Engine): The database engine to use for the query.

    Returns:
    List[pandas.DataFrame]: A list of DataFrames containing the fetched data.
    """

    dfs = []
    for company_id in company_ids:
        df = get_company_data(table_name, company_id, start_date, end_date, engine)
        dfs.append(df)

    return dfs


def get_all_companies(engine):
    """
    Fetch all companies from the database.

    Parameters:
    engine (sqlalchemy.engine.Engine): The database engine to use for the query.

    Returns:
    pandas.DataFrame: A DataFrame containing all companies.
    """
    query = "SELECT c.*, m.name as market FROM companies c INNER JOIN markets m on m.id=c.mid ORDER BY name;"
    df = pd.read_sql_query(query, engine)
    return df
