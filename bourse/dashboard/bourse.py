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


DATABASE_URI = "timescaledb://ricou:monmdp@db:5432/bourse"  # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse' # outisde docker

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
    "1D": 1,
    "1W": 7,
    "1M": 30,
    "3M": 90,
    "6M": 182,
    "1Y": 365,
    "2Y": 730,
    "5Y": 1825,
    "10Y": 3650,
}

time_interval_options = [{"label": key, "value": value} for key, value in time_intervals.items()]

app = dash.Dash(
    __name__,
    title="Bourse",
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.QUARTZ],
)

server = app.server

app.layout = html.Div(
    [
        html.H1(
            children="Bourse", style={"textAlign": "center", "marginBottom": "30px"}
        ),
        html.Div(
            [
                html.Div(
                    [
                        dcc.DatePickerRange(
                            id="date-picker-range",
                            start_date=START_DATE,
                            end_date=END_DATE,
                            min_date_allowed=START_DATE,
                            max_date_allowed=END_DATE,
                            initial_visible_month="2019-01-01",
                            display_format="YYYY-MM-DD",
                            style={"marginRight": "20px"},
                        ),
                        daq.ToggleSwitch(
                            id="use-date-picker-range",
                            value=False,
                            style={"marginRight": "20px"},
                        ),
                        dcc.Dropdown(
                            id="time-interval",
                            options=time_interval_options,
                            value=1,
                            clearable=False,
                            style={"width": "150px !important", "marginRight": "20px"},
                        ),
                        dcc.DatePickerSingle(
                            id="date-picker-single",
                            date=END_DATE,
                            display_format="YYYY-MM-DD",
                        ),
                    ],
                    style={"marginBottom": "20px", "display": "flex", "alignItems": "center"},
                ),
                html.Div(
                    [
                        dcc.Dropdown(
                            id="market-dropdown",
                            options=market_options,
                            value="all",
                            searchable=True,
                            placeholder="Search and select a market...",
                            style={"width": "300px", "marginRight": "20px"},
                        ),
                        dcc.Dropdown(
                            id="company-dropdown",
                            options=company_options,
                            value=[],
                            multi=True,
                            searchable=True,
                            placeholder="Search and select companies...",
                            style={"width": "300px", "marginRight": "20px"},
                        ),
                        dcc.Dropdown(
                            id="selected-company-dropdown",
                            options=[],
                            value=None,
                            searchable=True,
                            placeholder="Select a company...",
                            style={"width": "300px"},
                        ),
                    ],
                    style={"marginBottom": "20px", "display": "flex", "alignItems": "center"},
                ),
            ],
            style={"display": "flex", "justifyContent": "space-between"},
        ),
        html.Div(
            [
                html.Div(
                    [
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
                            style={"height": "500px"},
                        )
                    ],
                    style={"flex": 3, "marginRight": "20px"},
                ),
                html.Div(
                    [
                        dash_table.DataTable(
                            id="raw-data-table",
                            columns=[
                                {"name": col, "id": col}
                                for col in [
                                    "date",
                                    "company",
                                    "open",
                                    "high",
                                    "low",
                                    "close",
                                    "volume",
                                    "mean",
                                    "std",
                                ]
                            ],
                            style_cell={"textAlign": "left"},
                            style_table={"overflowY": "scroll", "maxHeight": "500px"},
                        )
                    ],
                    style={"flex": 2},
                ),
            ],
            style={"display": "flex", "marginTop": "20px"},
        ),
    ],
    style={"margin": "0 auto", "padding": "20px"},
)

@app.callback(
    [
        Output("graph-content", "figure"),
        Output("raw-data-table", "data"),
        Output("selected-company-dropdown", "options"),
    ],
    [
        Input("market-dropdown", "value"),
        Input("company-dropdown", "value"),
        Input("selected-company-dropdown", "value"),
        Input("graph-type", "value"),
        Input("use-date-picker-range", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("date-picker-single", "date"),
        Input("time-interval", "value"),
    ],
)
def update_graph(
    selected_market,
    company_ids,
    selected_company_id,
    graph_type,
    use_date_picker_range,
    start_date,
    end_date,
    single_date,
    time_interval,
):      
    if selected_market == "all":
        filtered_companies = df_companies
    else:
        filtered_companies = df_companies[df_companies["market"] == selected_market]

    company_options = [
        {"label": label, "value": id}
        for label, id in zip(filtered_companies["labels"], filtered_companies["id"])
    ]

    selected_company_options = [
        {"label": option["label"], "value": option["value"]}
        for option in company_options
        if option["value"] in company_ids
    ]

    if use_date_picker_range:
        end_date = single_date
        days = time_interval
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
        figure = stocks.generate_bollinger_graph(
            engine, selected_company_id, start_date, end_date
        )

    raw_data = stocks.generate_raw_data_table(engine, company_ids, start_date, end_date)
    return figure, raw_data, selected_company_options

@app.callback(
    Output("date-picker-range", "disabled"),
    Output("date-picker-single", "disabled"),
    Output("time-interval", "disabled"),
    Input("use-date-picker-range", "value"),
)
def switch_date_picker(use_date_picker_range):
    if use_date_picker_range:
        return True, False, False
    else:
        return False, True, True


if __name__ == "__main__":
    print("Starting app")
    app.run(debug=True)
