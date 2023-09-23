import pandas as pd
from dash import Dash, dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import geopandas as gpd 
import plotly.express as px

geo_data = gpd.read_file('data/geo_data.geojson')
geo_data.geometry = geo_data.geometry.to_crs(epsg = 4326)

fig = px.choropleth_mapbox(geo_data,
                           geojson=geo_data.__geo_interface__,
                           locations=geo_data.geoid,
                           color='Value',
                           hover_data=["DEN_PROV"],
                           labels={'Value':'Number of people'},
                           featureidkey='properties.geoid',
                           center={'lat': 41.93309, 'lon': 12.13129},
                           mapbox_style='carto-positron',
                           zoom=3.8,
                           opacity=0.4,
                           )


fig.update_layout(margin={'l': 0, 't': 0, 'r': 0, 'b': 0})
fig.update_coloraxes(
    colorbar=dict(
        lenmode="fraction",
        len=0.4,
        x=0,
        y=0.25
    )
)

app = Dash(__name__, title = 'Page test',
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)
server = app.server

app.layout = dbc.Container([

    # Maps
    dcc.Graph(id='map', figure = fig, style={'width': '98vw', 'height': '96vh'}),

], fluid=True)

if __name__ == '__main__':
    app.run_server(debug=True)
    