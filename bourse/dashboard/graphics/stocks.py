import pandas as pd
import plotly.graph_objects as go
import graphics.database_operations as db_ops


def generate_line_graph(engine, company_ids, start_date, end_date):
    dfs_stocks = db_ops.get_companies_data(
        "stocks", company_ids, start_date, end_date, engine
    )

    data = []
    for df in dfs_stocks:
        data.append(
            {
                "x": df["date"],
                "y": df["value"],
                "type": "line",
                "name": df["name"].iloc[0],
            }
        )

    figure = {"data": data, "layout": {"title": "Stocks", "yaxis": {"type": "log"}}}
    return figure


def generate_candlestick_graph(engine, company_id, start_date, end_date):
    df = db_ops.get_company_data("daystocks", company_id, start_date, end_date, engine)

    figure = go.Figure(
        data=[
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
            )
        ]
    )
    return figure
