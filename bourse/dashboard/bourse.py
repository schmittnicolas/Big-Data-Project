import dash
from dash import dcc
from dash import html
import graphics.stocks as stocks
import graphics.database_operations as db_ops
import dash.dependencies as ddep
import sqlalchemy
import pandas as pd
import dash_daq as daq

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

DATABASE_URI = "timescaledb://ricou:monmdp@db:5432/bourse"  # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

df_companies = db_ops.get_all_companies(engine)
df_companies["labels"] = df_companies["name"] + " (" + df_companies["symbol"] + ")"

company_options = [
    {"label": label, "value": id}
    for label, id in zip(df_companies["labels"], df_companies["id"])
]

start_date = "2019-01-01"
end_date = "2019-02-01"

app = dash.Dash(
    __name__, title="Bourse", suppress_callback_exceptions=True
)  # , external_stylesheets=external_stylesheets)
server = app.server

# TODO Creer une liste mes symboles

app.layout = html.Div(
    [
        html.H1(
            children="Bourse",
        ),
        daq.ToggleSwitch(
            id="toggle-switch",
            value=False,
            label="Candlestick",
            labelPosition="top",
        ),
        dcc.DatePickerRange(
            id="date-picker",
            start_date=start_date,
            end_date=end_date,
            min_date_allowed="2019-01-01",
            max_date_allowed="2024-12-31",
            initial_visible_month="2019-01-01",
            display_format="YYYY-MM-DD",
        ),
        dcc.Graph(id="graph-content", config={"displayModeBar": False}),
        html.Div(
            [
                html.Label("Select Companies:", style={"fontWeight": "bold"}),
                dcc.Checklist(
                    id="checklist-selection",
                    options=company_options,
                    value=[],
                ),
            ],
        ),
    ],
)


@app.callback(
    ddep.Output("graph-content", "figure"),
    [
        ddep.Input("checklist-selection", "value"),
        ddep.Input("date-picker", "start_date"),
        ddep.Input("date-picker", "end_date"),
        ddep.Input("toggle-switch", "value"),
    ],
)
def update_graph(company_ids, start_date, end_date, is_candlestick):
    if is_candlestick:
        return stocks.generate_candlestick_graph(
            engine, company_ids[0], start_date, end_date
        )
    else:
        return stocks.generate_line_graph(engine, company_ids, start_date, end_date)


if __name__ == "__main__":
    print("Starting app")
    app.run(debug=True)
