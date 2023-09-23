from dash import dcc, html
from dash import html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import pandas as pd

menuHome = html.Div([
    dbc.Row(children=[

        dcc.Dropdown(['Uno', 'Dos', 'Tres'], 'Uno', id='selectNumero'),

        html.Br(),

        dcc.Dropdown({f'{i}': f'{i}' for i in ['SF', 'Montreal']}, 'Montreal', id='selectMenu'),

        html.Br(),  

        dbc.Button("Apply", id="btn_menu", className="btn-platzi"),

    ]),
])

@callback(Output('filter_data', 'data'), 
          Input("btn_menu", "n_clicks"),
          State('selectMenu', 'value'),
          State('original_data', 'data'),
          prevent_initial_call=True,
          memoize=True
          )
def clean_data(n_clicks, value, data):

    if n_clicks is None:
        data = pd.read_json(data)
        data = data[data['City'] == value]
        return data.to_json(date_format='iso', orient='split') 
    else:
        data = pd.read_json(data)
        data = data[data['City'] == value]
        return data.to_json(date_format='iso', orient='split') 
