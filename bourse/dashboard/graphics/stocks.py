import pandas as pd
import plotly.graph_objects as go
import graphics.database_operations as db_ops
from plotly.offline import iplot
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

layout = dict(
    hovermode="closest",
    margin=dict(r=0, l=0, t=0, b=0),
    plot_bgcolor="#252e3f",
    paper_bgcolor="#252e3f",
    xaxis=dict(gridcolor="#7fafdf", gridwidth=1, showgrid=True, color="white"),
    yaxis=dict(gridcolor="#7fafdf", gridwidth=1, showgrid=True, color="white"),
    legend=dict(font=dict(color="white")),
    font=dict(color="white"),
)


def generate_raw_data_table(engine, company_ids, start_date, end_date):
    data = []
    for company_id in company_ids:
        df = db_ops.get_company_data(
            "daystocks", company_id, start_date, end_date, engine
        )
        if not df.empty:
            df = df.rename(columns={"name": "company"})
            df = df[
                [
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "company",
                    "mean",
                    "standard_deviation",
                ]
            ]
            df = df.fillna(0)
            data.append(df)

    if data:
        table_data = pd.concat(data, ignore_index=True)
        table_data = table_data[
            [
                "date",
                "company",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "mean",
                "standard_deviation",
            ]
        ]
        table_data = table_data.sort_values(by=["date", "company"])
        table_data["date"] = table_data["date"].dt.strftime("%Y-%m-%d")
        table_data = table_data.round(2)
        return table_data.to_dict("records")
    else:
        return []


def generate_line_graph(engine, company_ids, start_date, end_date):
    data = []
    dfs_stocks = db_ops.get_companies_data(
        "stocks", company_ids, start_date, end_date, engine
    )

    for df in dfs_stocks:
        if not df.empty:
            company_name = df["name"].iloc[0]
            data.append(
                go.Scatter(
                    x=df["date"],
                    y=df["value"],
                    mode="lines",
                    name=company_name,
                    hovertext=company_name,
                )
            )

    figure = go.Figure(data=data)
    figure.update_yaxes(type="log")

    figure.update_layout(layout)

    return figure


def generate_candlestick_graph(engine, company_ids, start_date, end_date):
    data = []
    for company_id in company_ids:
        df = db_ops.get_company_data(
            "daystocks", company_id, start_date, end_date, engine
        )
        if not df.empty:
            data.append(
                go.Candlestick(
                    x=df["date"],
                    open=df["open"],
                    high=df["high"],
                    low=df["low"],
                    close=df["close"],
                    name=df["name"].iloc[0],
                    line=dict(width=1),
                )
            )

    figure = go.Figure(data=data)
    figure.update_layout(layout)
    return figure


def generate_bollinger_graph(engine, company_id, start_date, end_date, window=30):
    extended_start_date = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(
        days=window * 2
    )
    df = db_ops.get_company_data(
        "daystocks",
        company_id,
        extended_start_date.strftime("%Y-%m-%d"),
        end_date,
        engine,
    )

    if df.empty:
        return go.Figure().update_layout(layout)

    df["sma"] = df["close"].rolling(window).mean()
    df["std"] = df["close"].rolling(window).std(ddof=0)

    df = df[(df["date"] >= start_date)]

    # Create subplots with 2 rows; top for candlestick price, and bottom for bar volume
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        subplot_titles=(df["name"].iloc[0], "Volume"),
        vertical_spacing=0.1,
        row_width=[0.2, 0.7],
    )

    # ----------------
    # Candlestick Plot
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            showlegend=False,
            name="candlestick",
        ),
        row=1,
        col=1,
    )

    # Moving Average
    fig.add_trace(
        go.Scatter(x=df["date"], y=df["sma"], line_color="black", name="sma"),
        row=1,
        col=1,
    )

    # Upper Bound
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["sma"] + (df["std"] * 2),
            line_color="gray",
            line={"dash": "dash"},
            name="upper band",
            opacity=0.5,
        ),
        row=1,
        col=1,
    )

    # Lower Bound fill in between with parameter 'fill': 'tonexty'
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["sma"] - (df["std"] * 2),
            line_color="gray",
            line={"dash": "dash"},
            fill="tonexty",
            name="lower band",
            opacity=0.5,
        ),
        row=1,
        col=1,
    )

    # ----------------
    # Volume Plot
    fig.add_trace(go.Bar(x=df["date"], y=df["volume"], showlegend=False), row=2, col=1)

    # Remove range slider; (short time frame)
    fig.update(layout_xaxis_rangeslider_visible=False)
    fig.update_layout(layout)
    return fig
