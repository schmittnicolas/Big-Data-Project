import dash
from dash import dcc
from dash import html
import graphics.stocks as stocks
import graphics.database_operations as db_ops
import dash.dependencies as ddep
import sqlalchemy
import pandas as pd
import dash_daq as daq
from dash.dependencies import Output, Input, State
import dash_bootstrap_components as dbc
from dash import Dash, dash_table
from datetime import datetime, timedelta


DATABASE_URI = "timescaledb://ricou:monmdp@db:5432/bourse"

engine = sqlalchemy.create_engine(DATABASE_URI)

df_companies = db_ops.get_all_companies(engine)

df_companies["labels"] = df_companies["name"] + " (" + df_companies["symbol"] + ")"
company_options = [
    {"label": label, "value": id}
    for label, id in zip(df_companies["labels"], df_companies["id"])
]

markets = df_companies["market"].unique()
market_options = [{"label": market, "value": market} for market in markets]

START_DATE = "2019-01-01"
END_DATE = "2024-12-31"


time_intervals = {
    1: ("1D", 1),
    2: ("1W", 7),
    3: ("1M", 30),
    4: ("3M", 90),
    5: ("6M", 182),
    6: ("1Y", 365),
    7: ("2Y", 730),
    8: ("5Y", 1825),
    9: ("10Y", 3650),
}

app = dash.Dash(
    __name__,
    title="BOURSE",
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)

server = app.server

app.layout = html.Div(
    id="root",
    children=[
        html.Div(
            id="header",
            children=[
                html.H4(children="Bourse Application"),
                html.P(
                    id="description",
                    children="Navigate through the different markets and companies to visualize their stock data.",
                ),
            ],
        ),
        html.Div(
            id="app-container",
            children=[
                html.Div(
                    id="left-column",
                    children=[
                        html.Div(
                            id="slider-container",
                            children=[
                                html.Div(
                                    [
                                        dcc.Dropdown(
                                            id="market-dropdown",
                                            options=market_options,
                                            multi=True,
                                            value="all",
                                            searchable=True,
                                            placeholder="Search and select markets...",
                                            style={
                                                "width": "300px",
                                                "marginRight": "20px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="company-dropdown",
                                            options=company_options,
                                            value=[],
                                            multi=True,
                                            searchable=True,
                                            placeholder="Search and select companies...",
                                            style={
                                                "width": "300px",
                                                "marginRight": "20px",
                                            },
                                        ),
                                        dcc.Dropdown(
                                            id="selected-company-dropdown",
                                            options=[],
                                            value=None,
                                            searchable=True,
                                            placeholder="Bollinger Bands Company...",
                                            style={"width": "300px"},
                                        ),
                                    ],
                                    style={
                                        "marginBottom": "20px",
                                        "display": "flex",
                                        "alignItems": "center",
                                    },
                                ),
                            ],
                        ),
                        html.Div(
                            id="heatmap-container",
                            children=[
                                dcc.Tabs(
                                    id="graph-type",
                                    value="line",
                                    children=[
                                        dcc.Tab(
                                            label="Default (Line)",
                                            value="line",
                                            style={"padding": "6px"},
                                        ),
                                        dcc.Tab(
                                            label="Candlestick Chart",
                                            value="candlestick",
                                            style={"padding": "6px"},
                                        ),
                                        dcc.Tab(
                                            label="Bollinger Bands",
                                            value="bollinger",
                                            style={"padding": "6px"},
                                        ),
                                    ],
                                    style={"width": "100%"},
                                ),
                                dcc.Graph(
                                    id="graph-content",
                                    config={"displayModeBar": False},
                                ),
                            ],
                        ),
                    ],
                ),
                html.Div(
                    id="graph-container",
                    children=[
                        html.Div(
                            dcc.Tabs(
                                [
                                    dcc.Tab(
                                        label="Date Range",
                                        value="range",
                                        children=[
                                            html.Div(
                                                dcc.DatePickerRange(
                                                    id="date-picker-range",
                                                    start_date=START_DATE,
                                                    end_date=END_DATE,
                                                    min_date_allowed=START_DATE,
                                                    max_date_allowed=END_DATE,
                                                    initial_visible_month="2019-01-01",
                                                    display_format="YYYY-MM-DD",
                                                    style={
                                                        "margin": "auto",
                                                    },
                                                ),
                                                style={
                                                    "display": "flex",
                                                    "justifyContent": "center",
                                                },
                                            ),
                                        ],
                                        style={
                                            "padding": "6px",
                                        },
                                    ),
                                    dcc.Tab(
                                        label="Interval",
                                        value="interval",
                                        children=html.Div(
                                            [
                                                html.Div(
                                                    dcc.Slider(
                                                        min=1,
                                                        max=9,
                                                        step=1,
                                                        marks={
                                                            i: time_intervals[i][0]
                                                            for i in time_intervals
                                                        },
                                                        value=1,
                                                        id="time-interval",
                                                        updatemode="drag",
                                                    ),
                                                    style={"width": "300px"},
                                                ),
                                                dcc.DatePickerSingle(
                                                    id="date-picker-single",
                                                    date=END_DATE,
                                                    display_format="YYYY-MM-DD",
                                                ),
                                            ],
                                            style={
                                                "display": "flex",
                                                "justifyContent": "center",
                                                "alignItems": "center",
                                            },
                                        ),
                                        style={
                                            "padding": "6px",
                                        },
                                    ),
                                ],
                                id="date-tabs",
                                style={"marginBottom": "20px", "width": "100%"},
                            ),
                            style={"marginBottom": "20px"},
                            id="date-picker-container",
                        ),
                        dash_table.DataTable(
                            id="raw-data-table",
                            columns=[
                                {
                                    "name": (
                                        col if col != "standard_deviation" else "std"
                                    ),
                                    "id": col,
                                }
                                for col in [
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
                            ],
                            style_header={
                                "whiteSpace": "normal",
                                "height": "auto",
                                "textAlign": "left",
                            },
                            style_table={
                                "overflowY": "scroll",
                                "maxHeight": "500px",
                            },
                            fixed_rows={"headers": True},
                        ),
                    ],
                ),
            ],
        ),
    ],
)


@app.callback(
    [
        Output("graph-content", "figure"),
        Output("raw-data-table", "data"),
        Output("company-dropdown", "options"),
        Output("selected-company-dropdown", "options"),
    ],
    [
        Input("market-dropdown", "value"),
        Input("company-dropdown", "value"),
        Input("selected-company-dropdown", "value"),
        Input("graph-type", "value"),
        Input("date-tabs", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("date-picker-single", "date"),
        Input("time-interval", "value"),
    ],
)
def update_graph(
    selected_markets,
    company_ids,
    selected_company_id,
    graph_type,
    active_date_tab,
    start_date,
    end_date,
    single_date,
    time_interval,
):
    filtered_companies = df_companies

    if selected_markets is not None and selected_markets != "all":
        filtered_companies = df_companies[df_companies["market"].isin(selected_markets)]

    company_options = [
        {"label": label, "value": id}
        for label, id in zip(filtered_companies["labels"], filtered_companies["id"])
    ]

    selected_company_options = [
        {"label": option["label"], "value": option["value"]}
        for option in company_options
        if option["value"] in company_ids
    ]

    if active_date_tab == "interval":
        end_date = single_date
        days = time_intervals[time_interval][1]
        start_date = (
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)
        ).strftime("%Y-%m-%d")

    figure = None
    if graph_type == "line":
        figure = stocks.generate_line_graph(engine, company_ids, start_date, end_date)
    elif graph_type == "candlestick":
        figure = stocks.generate_candlestick_graph(
            engine, company_ids, start_date, end_date
        )
    elif graph_type == "bollinger":
        if selected_company_id is None and company_ids:
            selected_company_id = company_ids[0]
        figure = stocks.generate_bollinger_graph(
            engine, selected_company_id, start_date, end_date
        )

    raw_data = stocks.generate_raw_data_table(engine, company_ids, start_date, end_date)
    return figure, raw_data, company_options, selected_company_options


if __name__ == "__main__":
    print("Starting app")
    app.run(debug=True)
